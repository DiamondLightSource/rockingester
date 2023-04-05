import asyncio
import glob
import logging
import os
import time
from typing import List

from dls_utilpack.callsign import callsign
from dls_utilpack.explain import explain2
from dls_utilpack.require import require
from PIL import Image

# Dataface client context.
from xchembku_api.datafaces.context import Context as XchembkuDatafaceClientContext

# Crystal well pydantic model.
from xchembku_api.models.crystal_well_model import CrystalWellModel

# Base class for collector instances.
from rockingest_lib.collectors.base import Base as CollectorBase

logger = logging.getLogger(__name__)

thing_type = "rockingest_lib.collectors.direct_poll"


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
        self.__directories = require(s, type_specific_tbd, "directories")
        self.__recursive = require(s, type_specific_tbd, "recursive")

        # We will use the dataface to discover previously processed files.
        # We will also discovery newly find files into this database.
        self.__xchembku_client_context = None
        self.__xchembku = None

        # This flag will stop the ticking async task.
        self.__keep_ticking = True
        self.__tick_future = None

        self.__known_filenames = []

    # ----------------------------------------------------------------------------------------
    async def activate(self) -> None:
        """
        Activate the object.

        This implementation gets the list of filenames already known to the xchembku.

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

        # Get all the jobs ever done.
        # TODO: Avoid needing to fetch all rockingest records and matching to all disk files.
        models: List[
            CrystalWellModel
        ] = await self.__xchembku.fetch_crystal_wells_filenames(
            why="rockingest activate getting all crystal wells ever done"
        )

        # Make an initial list of the data labels associated with any job.
        self.__known_filenames = []
        for model in models:
            if model.filename not in self.__known_filenames:
                self.__known_filenames.append(model.filename)

        logger.debug(f"activating with {len(models)} known filenames")

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
                await self.scrape()
            except Exception as exception:
                logger.error(explain2(exception, "scraping"), exc_info=exception)

            # TODO: Make periodic tick period to be configurable.
            await asyncio.sleep(1.0)

    # ----------------------------------------------------------------------------------------
    async def scrape(self) -> None:
        """
        Scrape all the configured directories looking for new files.
        """

        collection: List[CrystalWellModel] = []

        # TODO: Use asyncio tasks to parellize scraping directories.
        for directory in self.__directories:
            await self.scrape_directory(directory, collection)

        # Flush any remaining collection to the database.
        await self.flush_collection(collection)

    # ----------------------------------------------------------------------------------------
    async def scrape_directory(
        self,
        directory: str,
        collection: List[CrystalWellModel],
    ) -> None:
        """
        Scrape a single directory looking for new files.

        Adds discovered files to internal list which gets pushed when it reaches a configurable size.

        Also add discovered files to internal list of known files to avoid duplicate pushing.
        """

        if not os.path.isdir(directory):
            return

        t0 = time.time()
        filenames = glob.glob(f"{directory}/**", recursive=self.__recursive)
        t1 = time.time()

        new_count = 0
        for filename in filenames:
            if os.path.isdir(filename):
                continue

            if filename not in self.__known_filenames:
                # Add image to list of collection.
                await self.add_discovery(filename, collection)
                self.__known_filenames.append(filename)
                new_count = new_count + 1

        if new_count >= 0:
            seconds = "%0.3f" % (t1 - t0)
            logger.info(
                f"from {directory} found {new_count} newly actionable files"
                f" among {len(filenames)} total files in {seconds} seconds"
            )

    # ----------------------------------------------------------------------------------------
    async def add_discovery(
        self,
        filename: str,
        collection: List[CrystalWellModel],
    ) -> None:
        """
        Add new discovery for later flush.
        """

        if len(collection) >= 1000:
            await self.flush_collection(collection)

        error = None
        try:
            image = Image.open(filename)
            width, height = image.size
        except Exception as exception:
            error = str(exception)
            width = None
            height = None

        # Add a new discovery to the collection.
        collection.append(
            CrystalWellModel(
                filename=filename,
                error=error,
                width=width,
                height=height,
            )
        )

    # ----------------------------------------------------------------------------------------
    async def flush_collection(self, collection: List[CrystalWellModel]) -> None:
        """
        Send the discovered files to xchembku for storage.
        """

        if len(collection) == 0:
            return

        logger.debug(f"flushing {len(collection)} from collection")

        # Here we originate the crystal well records into xchembku.
        await self.__xchembku.originate_crystal_wells(collection)

        collection.clear()

    # ----------------------------------------------------------------------------------------
    async def close_client_session(self):
        """"""

        pass
