import asyncio
import logging
import os
import shutil
from pathlib import Path
from typing import Dict, List

from dls_utilpack.callsign import callsign
from dls_utilpack.explain import explain2
from dls_utilpack.require import require
from dls_utilpack.visit import VisitNotFound, get_xchem_directory
from PIL import Image

# Crystal plate object interface.
from xchembku_api.crystal_plate_objects.interface import (
    Interface as CrystalPlateInterface,
)

# Dataface client context.
from xchembku_api.datafaces.context import Context as XchembkuDatafaceClientContext
from xchembku_api.models.crystal_plate_filter_model import CrystalPlateFilterModel

# Crystal plate pydantic model.
from xchembku_api.models.crystal_plate_model import CrystalPlateModel

# Crystal well pydantic model.
from xchembku_api.models.crystal_well_model import CrystalWellModel

# Crystal plate objects factory.
from xchembku_lib.crystal_plate_objects.crystal_plate_objects import CrystalPlateObjects

# Base class for collector instances.
from rockingester_lib.collectors.base import Base as CollectorBase

logger = logging.getLogger(__name__)

thing_type = "rockingester_lib.collectors.direct_poll"


# ------------------------------------------------------------------------------------------
class DirectPoll(CollectorBase):
    """
    Object representing an image collector.
    The behavior is to start a coro task to waken every few seconds and scan for newly created plate directories.
    Image files are pushed to xchembku.
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

        # Explicit list of barcodes to process (used when testing a deployment).
        self.__ingest_only_barcodes = type_specific_tbd.get("ingest_only_barcodes")

        # Database where we will get plate barcodes and add new wells.
        self.__xchembku_client_context = None
        self.__xchembku = None

        # This flag will stop the ticking async task.
        self.__keep_ticking = True
        self.__tick_future = None

        # This is the last formulatrix plate we have ingested, used to avoid re-handling the same plate.
        self.__latest_formulatrix__plate__id = 0

        # This is the list of plates indexed by their barcode.
        self.__crystal_plate_models_by_barcode: Dict[CrystalPlateModel] = {}

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
            try:
                # Fetch all the plates we don't have yet.
                await self.fetch_plates_by_barcode()

                # Scrape all the configured plates directories.
                await self.scrape()
            except Exception as exception:
                logger.error(explain2(exception, "scraping"), exc_info=exception)

            # TODO: Make periodic tick period to be configurable.
            await asyncio.sleep(1.0)

    # ----------------------------------------------------------------------------------------
    async def fetch_plates_by_barcode(self) -> None:
        """
        Fetch all barcodes in the database which we don't have in memory yet.
        """

        # Fetch all the plates we don't have yet.
        plate_models = await self.__xchembku.fetch_crystal_plates(
            CrystalPlateFilterModel(
                from_formulatrix__plate__id=self.__latest_formulatrix__plate__id
            ),
            why="[ROCKINGESTER POLL]",
        )

        # Add the plates to the list, assumed sorted by formulatrix__plate__id.
        for plate_model in plate_models:
            self.__crystal_plate_models_by_barcode[plate_model.barcode] = plate_model

            # Remember the highest one we got to shorten the query for next time.
            self.__latest_formulatrix__plate__id = plate_model.formulatrix__plate__id

    # ----------------------------------------------------------------------------------------
    async def scrape(self) -> None:
        """
        Scrape all the configured directories looking for new files.
        """

        # TODO: Use asyncio tasks to parellize scraping plates directories.
        for directory in self.__plates_directories:
            await self.scrape_plates_directory(Path(directory))

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
            # We already handled this plate name?
            if plate_name in self.__handled_plate_names:
                # logger.debug(
                #     f"[ROCKINGESTER POLL] plate_barcode {plate_barcode}"
                #     f" is already handled in this instance"
                # )
                continue

            # Get the plate's barcode from the directory name.
            plate_barcode = plate_name[0:4]

            # We have a specific list we want to process?
            if self.__ingest_only_barcodes is not None:
                if plate_barcode not in self.__ingest_only_barcodes:
                    continue

            # Get the matching plate record from the database.
            crystal_plate_model = self.__crystal_plate_models_by_barcode.get(
                plate_barcode
            )

            # This plate directory's barcode is not in the database?
            if crystal_plate_model is None:
                logger.debug(
                    f"[ROCKDIR] plate_barcode {plate_barcode} is not in the database"
                )
                self.__handled_plate_names.append(plate_name)
                continue
            try:
                # Try to get the visit directory from the visit stored in the database's plate record.
                visit_directory = Path(
                    get_xchem_directory(
                        self.__visits_directory, crystal_plate_model.visit
                    )
                )
            # This is an improperly formatted visit name?
            except ValueError:
                logger.debug(
                    f"[ROCKDIR] plate_barcode {plate_barcode}"
                    f" is in the database, but visit {crystal_plate_model.visit} is improperly formed"
                )
                self.__handled_plate_names.append(plate_name)
                continue
            # This visit is not found on disk?
            except VisitNotFound:
                logger.debug(
                    f"[ROCKDIR] plate_barcode {plate_barcode}"
                    f" is in the database, but cannot find visit directory in {self.__visits_directory}"
                )
                self.__handled_plate_names.append(plate_name)
                continue

            # Scrape the directory when all image files have arrived.
            await self.scrape_plate_directory_if_complete(
                plates_directory / plate_name,
                crystal_plate_model,
                visit_directory,
            )

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
                f"[ROCKDIR] plate_barcode {plate_directory.name} is apparently already copied to {target}"
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

        # Get all the well images in the plate directory.
        subwell_names = [
            entry.name for entry in os.scandir(plate_directory) if entry.is_file()
        ]

        # Make an object corresponding to the crystal plate model's type.
        crystal_plate_object = CrystalPlateObjects().build_object(
            {"type": crystal_plate_model.thing_type}
        )

        # Don't handle the plate directory until all images have arrived.
        # TODO: Put in some kind of failsafe in direct_poll.py to handle case where all the well images never arrive.
        if len(subwell_names) < crystal_plate_object.get_well_count():
            logger.debug(
                f"[PLATEWAIT] found only {len(subwell_names)} out of {crystal_plate_object.get_well_count()} subwell images in {plate_directory}"
            )
            return

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
