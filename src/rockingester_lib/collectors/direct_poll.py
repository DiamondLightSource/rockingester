import asyncio
import logging
import os
import shutil
from pathlib import Path
from typing import Dict, List

from dls_utilpack.callsign import callsign
from dls_utilpack.explain import explain2
from dls_utilpack.require import require
from PIL import Image

# Dataface client context.
from xchembku_api.datafaces.context import Context as XchembkuDatafaceClientContext
from xchembku_api.models.crystal_plate_filter_model import CrystalPlateFilterModel

# Crystal plate pydantic model.
from xchembku_api.models.crystal_plate_model import CrystalPlateModel

# Crystal well pydantic model.
from xchembku_api.models.crystal_well_model import CrystalWellModel

# Base class for collector instances.
from rockingester_lib.collectors.base import Base as CollectorBase

logger = logging.getLogger(__name__)

thing_type = "rockingester_lib.collectors.direct_poll"


# ------------------------------------------------------------------------------------------
class DirectPoll(CollectorBase):
    """
    Object representing an image collector.
    The behavior is to start a coro task to waken every few seconds and scan for incoming files.
    Files are pushed to xchembku.
    """

    # ----------------------------------------------------------------------------------------
    def __init__(self, specification, predefined_uuid=None):
        CollectorBase.__init__(
            self, thing_type, specification, predefined_uuid=predefined_uuid
        )

        s = f"{callsign(self)} specification", self.specification()

        type_specific_tbd = require(s, self.specification(), "type_specific_tbd")
        self.__plates_directories = require(s, type_specific_tbd, "plates_directories")
        self.__ingested_directory = Path(
            require(s, type_specific_tbd, "ingested_directory")
        )
        self.__nobarcode_directory = Path(
            require(s, type_specific_tbd, "nobarcode_directory")
        )

        self.__ingest_only_barcodes = type_specific_tbd.get("ingest_only_barcodes")

        # Database where we will get plate barcodes and add new wells.
        self.__xchembku_client_context = None
        self.__xchembku = None

        # This flag will stop the ticking async task.
        self.__keep_ticking = True
        self.__tick_future = None

        self.__latest_formulatrix__plate__id = 0

        # This is the list of plates indexed by their barcode.
        self.__crystal_plate_models_by_barcode: Dict[CrystalPlateModel] = {}

    # ----------------------------------------------------------------------------------------
    async def activate(self) -> None:
        """
        Activate the object.

        Then it starts the coro task to awaken every few seconds to scrape the directories.
        """

        # Make sure the nobarcode_directory is created.
        try:
            self.__nobarcode_directory.mkdir(parents=True)
        except FileExistsError:
            pass

        # Make sure the ingested_directory is created.
        try:
            self.__ingested_directory.mkdir(parents=True)
        except FileExistsError:
            pass

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
            logger.debug(f"[ECHDON] {callsign(self)} exiting __xchembku_client_context")
            await self.__xchembku_client_context.aexit()
            logger.debug(f"[ECHDON] {callsign(self)} exited __xchembku_client_context")
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
            )
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

        logger.info(
            f"[SCRDIR] found {len(plate_names)} plate directories in {plates_directory}"
        )

        for plate_name in plate_names:
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

            # This plate is in the database?
            if crystal_plate_model is not None:
                await self.scrape_plate_directory(
                    plates_directory / plate_name,
                    crystal_plate_model,
                )
            # Not in the database?
            else:
                await self.move_to_nobarcode(plates_directory / plate_name)

    # ----------------------------------------------------------------------------------------
    async def move_to_nobarcode(
        self,
        plate_directory: Path,
    ) -> None:
        """
        Move a plate directory's well images to the "nobarcode" area.

        Then remove the plate directory.
        """

        target = self.__nobarcode_directory / plate_directory.name
        try:
            target.mkdir(parents=True)
        except FileExistsError:
            pass

        # Get all the well images in the plate directory.
        well_names = [
            entry.name for entry in os.scandir(plate_directory) if entry.is_file()
        ]

        for well_name in well_names:
            # Move to nobarcode, replacing what might already be there.
            # TODO: Protect against moving an image file which is currently being written by Luigi.
            shutil.move(
                plate_directory / well_name,
                target / well_name,
            )

        # Remove the source directory, which should now be empty.
        # TODO: Protect against removing an plate directory which is currently being written by Luigi.
        try:
            plate_directory.rmdir()
        except OSError:
            pass

    # ----------------------------------------------------------------------------------------
    async def scrape_plate_directory(
        self,
        plate_directory: Path,
        crystal_plate_model: CrystalPlateModel,
    ) -> None:
        """
        Scrape a single directory looking for new files.

        Adds discovered files to internal list which gets pushed when it reaches a configurable size.
        """

        # Get all the well images in the plate directory.
        well_names = [
            entry.name for entry in os.scandir(plate_directory) if entry.is_file()
        ]

        for well_name in well_names:
            # Process well's image file.
            await self.ingest_well(
                plate_directory,
                well_name,
                crystal_plate_model,
            )

        # Remove the source directory, which should now be empty.
        # TODO: Protect against removing an plate directory which is currently being written by Luigi.
        try:
            plate_directory.rmdir()
        except OSError:
            pass

    # ----------------------------------------------------------------------------------------
    async def ingest_well(
        self,
        plate_directory: Path,
        well_name: str,
        crystal_plate_model: CrystalPlateModel,
    ) -> None:
        """
        Ingest the well into the database.

        Move the well image file to the ingested area.

        TODO: Protect against ingesting an image file which is currently being written by Luigi.
        """

        well_filename = plate_directory / well_name
        error = None
        try:
            image = Image.open(well_filename)
            width, height = image.size
        except Exception as exception:
            error = str(exception)
            width = None
            height = None

        crystal_well_model = CrystalWellModel(
            filename=str(well_filename),
            crystal_plate_uuid=crystal_plate_model.uuid,
            error=error,
            width=width,
            height=height,
        )

        # Here we originate the crystal well records into xchembku.
        # TODO: Handle case where the same crystal well filename has already been originated.
        await self.__xchembku.originate_crystal_wells([crystal_well_model])

        target = self.__ingested_directory / plate_directory.name
        try:
            target.mkdir(parents=True)
        except FileExistsError:
            pass

        # Move to ingested, replacing what might already be there.
        shutil.move(
            well_filename,
            target / well_name,
        )

    # ----------------------------------------------------------------------------------------
    async def close_client_session(self):
        """"""

        pass
