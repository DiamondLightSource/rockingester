import asyncio
import logging
import os
import shutil
import time
from pathlib import Path
from typing import List

from dls_utilpack.callsign import callsign
from dls_utilpack.explain import explain2
from dls_utilpack.require import require
from dls_utilpack.visit import get_xchem_directory
from PIL import Image

# Crystal plate object interface.
from xchembku_api.crystal_plate_objects.interface import (
    Interface as CrystalPlateInterface,
)

# Dataface client context.
from xchembku_api.datafaces.context import Context as XchembkuDatafaceClientContext

# Crystal plate pydantic model.
from xchembku_api.models.crystal_plate_model import CrystalPlateModel

# Crystal well pydantic model.
from xchembku_api.models.crystal_well_model import CrystalWellModel

# Crystal plate objects factory.
from xchembku_lib.crystal_plate_objects.crystal_plate_objects import CrystalPlateObjects

# Base class for collector instances.
from rockingester_lib.collectors.base import Base as CollectorBase

# Object able to talk to the formulatrix database.
from rockingester_lib.ftrix_client import FtrixClient

# Object which can inject new xchembku plate records discovered while looking in subwell images.
from rockingester_lib.plate_injector import PlateInjector

logger = logging.getLogger(__name__)

thing_type = "rockingester_lib.collectors.direct_poll"


# ------------------------------------------------------------------------------------------
class DirectPoll(CollectorBase):
    """
    Object representing an image collector.
    The behavior is to start a coro task to waken every few seconds and scan for newly created plate directories.
    Image files are pushed to xchembku.
    Plates for the image files are also pushed to xchembku the first time they are wanted.
    """

    # ----------------------------------------------------------------------------------------
    def __init__(self, specification, predefined_uuid=None):
        CollectorBase.__init__(
            self, thing_type, specification, predefined_uuid=predefined_uuid
        )

        s = f"{callsign(self)} specification", self.specification()

        type_specific_tbd = require(s, self.specification(), "type_specific_tbd")

        # The sources for the collecting.
        self.__plates_directories = require(s, type_specific_tbd, "plates_directories")

        # The root directory of all visits.
        self.__visits_directory = Path(
            require(s, type_specific_tbd, "visits_directory")
        )

        # The subdirectory under a visit where to put subwell images that are collected.
        self.__visit_plates_subdirectory = Path(
            require(s, type_specific_tbd, "visit_plates_subdirectory")
        )

        # Reference the dict entry for the ftrix client specification.
        self.__ftrix_client_specification = require(
            s, type_specific_tbd, "ftrix_client_specification"
        )

        # Explicit list of barcodes to process (used when testing a deployment).
        self.__ingest_only_barcodes = type_specific_tbd.get("ingest_only_barcodes")

        # Maximum time to wait for final image to arrive, relative to time of last arrived image.
        self.__max_wait_seconds = require(s, type_specific_tbd, "max_wait_seconds")

        # Database where we will get plate barcodes and add new wells.
        self.__xchembku_client_context = None
        self.__xchembku = None

        # Object able to talk to the formulatrix database.
        self.__ftrix_client = None

        # This flag will stop the ticking async task.
        self.__keep_ticking = True
        self.__tick_future = None

        # The plate names which we have already finished handling within the current instance.
        self.__handled_plate_names = []

    # ----------------------------------------------------------------------------------------
    async def activate(self) -> None:
        """
        Activate the object.

        Then it starts the coro task to awaken every few seconds to scrape the directories.
        """

        # Make the xchembku client context.
        s = require(
            f"{callsign(self)} specification",
            self.specification(),
            "type_specific_tbd",
        )
        s = require(
            f"{callsign(self)} type_specific_tbd",
            s,
            "xchembku_dataface_specification",
        )
        self.__xchembku_client_context = XchembkuDatafaceClientContext(s)

        # Activate the context.
        await self.__xchembku_client_context.aenter()

        # Get a reference to the xchembku interface provided by the context.
        self.__xchembku = self.__xchembku_client_context.get_interface()

        # Object able to talk to the formulatrix database.
        self.__ftrix_client = FtrixClient(
            self.__ftrix_client_specification,
        )

        # Object which can inject new xchembku plate records discovered while looking in subwell images.
        self.__plate_injector = PlateInjector(
            self.__ftrix_client,
            self.__xchembku,
        )
        # Poll periodically.
        self.__tick_future = asyncio.get_event_loop().create_task(self.tick())

    # ----------------------------------------------------------------------------------------
    async def deactivate(self) -> None:
        """
        Deactivate the object.

        Causes the coro task to stop.

        This implementation then releases resources relating to the xchembku connection.
        """

        if self.__tick_future is not None:
            # Set flag to stop the periodic ticking.
            self.__keep_ticking = False
            # Wait for the ticking to stop.
            await self.__tick_future

        # Forget we have an xchembku client reference.
        self.__xchembku = None

        if self.__xchembku_client_context is not None:
            await self.__xchembku_client_context.aexit()
            self.__xchembku_client_context = None

    # ----------------------------------------------------------------------------------------
    async def tick(self) -> None:
        """
        A coro task which does periodic checking for new files in the directories.

        Stops when flag has been set by other tasks.

        # TODO: Use an event to awaken ticker early to handle stop requests sooner.
        """

        while self.__keep_ticking:
            # Scrape all the configured plates directories.
            await self.scrape_plates_directories()

            # TODO: Make periodic tick period to be configurable.
            await asyncio.sleep(1.0)

    # ----------------------------------------------------------------------------------------
    async def scrape_plates_directories(self) -> None:
        """
        Scrape all the configured directories looking in each one for new plate directories.

        Normally there is only one in the configured list of these places where plates arrive.
        """

        # TODO: Use asyncio tasks to paralellize scraping plates directories.
        for directory in self.__plates_directories:
            try:
                await self.scrape_plates_directory(Path(directory))
            except Exception as exception:
                # Just log the error, tag as anomaly for reporting, don't die.
                logger.error(
                    "[ANOMALY] "
                    + explain2(exception, f"scraping plates directory {directory}"),
                    exc_info=exception,
                )

    # ----------------------------------------------------------------------------------------
    async def scrape_plates_directory(
        self,
        plates_directory: Path,
    ) -> None:
        """
        Scrape a single directory looking for subdirectories which correspond to plates.
        """

        if not plates_directory.is_dir():
            return

        plate_names = [
            entry.name for entry in os.scandir(plates_directory) if entry.is_dir()
        ]

        # Make sure we scrape the plate directories in barcode-order, which is the same as date order.
        plate_names.sort()

        logger.debug(
            f"[ROCKINGESTER POLL] found {len(plate_names)} plate directories in {plates_directory}"
        )

        for plate_name in plate_names:
            try:
                await self.scrape_plate_directory(plates_directory / plate_name)
            except Exception as exception:
                # Just log the error, tag as anomaly for reporting, don't die.
                logger.error(
                    "[ANOMALY] "
                    + explain2(
                        exception,
                        f"scraping plate directory {str(plates_directory / plate_name)}",
                    ),
                    exc_info=exception,
                )

    # ----------------------------------------------------------------------------------------
    async def scrape_plate_directory(
        self,
        plate_directory: Path,
    ) -> None:
        """
        Scrape a single directory looking for images.
        """

        plate_name = plate_directory.name

        # We already handled this plate name?
        if plate_name in self.__handled_plate_names:
            # logger.debug(
            #     f"[ROCKINGESTER POLL] plate_barcode {plate_barcode}"
            #     f" is already handled in this instance"
            # )
            return

        # Get the plate's barcode from the directory name.
        plate_barcode = plate_name[0:4]

        # We have a specific list we want to process?
        if self.__ingest_only_barcodes is not None:
            if plate_barcode not in self.__ingest_only_barcodes:
                return

        # Get the matching plate record from the xchembku or formulatrix database.
        crystal_plate_model = await self.__plate_injector.find_or_inject_barcode(
            plate_barcode,
            self.__visits_directory,
        )

        # The model has not been marked as being in error?
        if crystal_plate_model.error is None:
            visit_directory = get_xchem_directory(
                self.__visits_directory, crystal_plate_model.visit
            )

            # Scrape the directory when all image files have arrived.
            await self.scrape_plate_directory_if_complete(
                plate_directory,
                crystal_plate_model,
                visit_directory,
            )

        # The model has been marked as being in error?
        else:
            logger.debug(
                f"[ROCKDIR] for plate_barcode {plate_barcode} crystal_plate_model.error is: {crystal_plate_model.error}"
            )
            # Remember we "handled" this one within the current instance.
            # Keeping this list could be obviated if we could move the files out of the plates directory after we process them.
            self.__handled_plate_names.append(plate_name)

    # ----------------------------------------------------------------------------------------
    async def scrape_plate_directory_if_complete(
        self,
        plate_directory: Path,
        crystal_plate_model: CrystalPlateModel,
        visit_directory: Path,
    ) -> None:
        """
        Scrape a single directory looking for new files.

        Adds discovered files to internal list which gets pushed when it reaches a configurable size.

        TODO: Consider some other flow where well images can be copied as they arrive instead of doing them all in a bunch.

        Args:
            plate_directory: disk directory where to look for subwell images
            crystal_plate_model: pre-built crystal plate description
            visit_directory: full path to the top of the visit directory
        """

        # Name of the destination directory where we will permanently store ingested well image files.
        target = (
            visit_directory / self.__visit_plates_subdirectory / plate_directory.name
        )

        # We have already put this plate directory into the visit directory?
        # This shouldn't really happen except when someone has been fiddling with the database.
        # TODO: Have a way to rebuild rockingest after database wipe, but images have already been copied to the visit.
        if target.is_dir():
            # Presumably this is done, so no error but log it.
            logger.debug(
                f"[ROCKDIR] plate directory {plate_directory.name} is apparently already copied to {target}"
            )
            self.__handled_plate_names.append(plate_directory.stem)
            return

        # This is the first time we have scraped a directory for this plate record in the database?
        if crystal_plate_model.rockminer_collected_stem is None:
            # Update the path stem in the crystal plate record.
            # TODO: Consider if important to report/record same barcodes on different rockmaker directories.
            crystal_plate_model.rockminer_collected_stem = plate_directory.stem
            await self.__xchembku.upsert_crystal_plates(
                [crystal_plate_model], "update rockminer_collected_stem"
            )

        # Get all the well images in the plate directory and the latest arrival time.
        subwell_names = []
        max_wait_seconds = self.__max_wait_seconds
        max_mtime = os.stat(plate_directory).st_mtime

        with os.scandir(plate_directory) as entries:
            for entry in entries:
                subwell_names.append(entry.name)
                max_mtime = max(max_mtime, entry.stat().st_mtime)

        # TODO: Verify that time.time() where rockingester runs matches os.stat() on filesystem from which images are collected.
        waited_seconds = time.time() - max_mtime

        # Make an object corresponding to the crystal plate model's type.
        crystal_plate_object = CrystalPlateObjects().build_object(
            {"type": crystal_plate_model.thing_type}
        )

        # Don't handle the plate directory until all images have arrived or some maximum wait has exceeded.
        if len(subwell_names) < crystal_plate_object.get_well_count():
            if waited_seconds < max_wait_seconds:
                logger.debug(
                    f"[PLATEWAIT] waiting longer since found only {len(subwell_names)}"
                    f" out of {crystal_plate_object.get_well_count()} subwell images"
                    f" in {plate_directory}"
                    f" after waiting {'%0.1f' % waited_seconds} out of {max_wait_seconds} seconds"
                )
                return
            else:
                logger.warning(
                    f"[PLATEDONE] done waiting even though found only {len(subwell_names)}"
                    f" out of {crystal_plate_object.get_well_count()} subwell images"
                    f" in {plate_directory}"
                    f" after waiting {'%0.1f' % waited_seconds} out of {max_wait_seconds} seconds"
                )
        else:
            logger.debug(
                f"[PLATEDONE] done waiting since found all {len(subwell_names)}"
                f" out of {crystal_plate_object.get_well_count()} subwell images"
                f" in {plate_directory}"
                f" after waiting {'%0.1f' % waited_seconds} out of {max_wait_seconds} seconds"
            )

        # Sort wells by name so that tests are deterministic.
        subwell_names.sort()

        crystal_well_models: List[CrystalWellModel] = []
        for subwell_name in subwell_names:
            # Make the well model, including image width/height.
            crystal_well_model = await self.ingest_well(
                plate_directory,
                subwell_name,
                crystal_plate_model,
                crystal_plate_object,
                target,
            )

            # Append well model to the list of all wells on the plate.
            crystal_well_models.append(crystal_well_model)

        # Here we create or update the crystal well records into xchembku.
        # TODO: Make sure that direct_poll does not double-create crystal well records if scrape is re-run with a different filename path.
        await self.__xchembku.upsert_crystal_wells(crystal_well_models)

        # Copy scraped directory to visit, replacing what might already be there.
        # TODO: Handle case where we upsert the crystal_well record but then unable to copy image file.
        shutil.copytree(
            plate_directory,
            target,
        )

        logger.info(
            f"copied {len(subwell_names)} well images from plate {plate_directory.name} to {target}"
        )

        # Remember we "handled" this one.
        self.__handled_plate_names.append(plate_directory.stem)

    # ----------------------------------------------------------------------------------------
    async def ingest_well(
        self,
        plate_directory: Path,
        subwell_name: str,
        crystal_plate_model: CrystalPlateModel,
        crystal_plate_object: CrystalPlateInterface,
        target: Path,
    ) -> CrystalWellModel:
        """
        Ingest the well into the database.

        Move the well image file to the ingested area.
        """

        input_well_filename = plate_directory / subwell_name
        ingested_well_filename = target / subwell_name

        # Stems are like "9acx_01A_1".
        # Convert the stem into a position as shown in soakdb3.
        position = crystal_plate_object.normalize_subwell_name(Path(subwell_name).stem)

        error = None
        try:
            image = Image.open(input_well_filename)
            width, height = image.size
        except Exception as exception:
            error = str(exception)
            width = None
            height = None

        crystal_well_model = CrystalWellModel(
            position=position,
            filename=str(ingested_well_filename),
            crystal_plate_uuid=crystal_plate_model.uuid,
            error=error,
            width=width,
            height=height,
        )

        return crystal_well_model

    # ----------------------------------------------------------------------------------------
    async def close_client_session(self):
        """"""

        pass
