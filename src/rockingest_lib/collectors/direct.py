import asyncio
import glob
import logging
import os
import time

from dls_utilpack.callsign import callsign
from dls_utilpack.describe import describe
from dls_utilpack.explain import explain2
from dls_utilpack.require import require
from PIL import Image
from xchembku_api.databases.constants import CrystalWellFieldnames, Tablenames

# Global dataface.
from xchembku_api.datafaces.datafaces import xchembku_datafaces_get_default

# Base class for collector instances.
from rockingest_lib.collectors.base import Base as CollectorBase

logger = logging.getLogger(__name__)

thing_type = "rockingest_lib.collectors.direct"


# ------------------------------------------------------------------------------------------
class Direct(CollectorBase):
    """
    Object representing a collector which launches a task using popen for onboard execution.
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
        self.__dataface = xchembku_datafaces_get_default()

        # This flag will stop the ticking async task.
        self.__keep_ticking = True
        self.__tick_future = None

        self.__known_filenames = []

    # ----------------------------------------------------------------------------------------
    async def activate(self):
        """"""

        # Get all the jobs ever done.
        records = await self.__dataface.query(
            f"SELECT {CrystalWellFieldnames.FILENAME} FROM {Tablenames.CRYSTAL_WELLS}"
        )

        # Make an initial list of the data labels associated with any job.
        self.__known_filenames = []
        for record in records:
            filename = record["filename"]
            if filename not in self.__known_filenames:
                self.__known_filenames.append(filename)

        logger.debug(f"activating with {len(records)} known filenames")

        # Poll periodically.
        self.__tick_future = asyncio.get_event_loop().create_task(self.tick())

    # ----------------------------------------------------------------------------------------
    async def deactivate(self):
        """"""

        if self.__tick_future is not None:
            # Set flag to stop the periodic ticking.
            self.__keep_ticking = False
            # Wait for the ticking to stop.
            await self.__tick_future

    # ----------------------------------------------------------------------------------------
    async def tick(self):
        """
        Periodic ticking to check for new work.
        """

        while self.__keep_ticking:
            try:
                await self.scrape()
            except Exception as exception:
                logger.error(explain2(exception, "scraping"), exc_info=exception)
            await asyncio.sleep(1.0)

    # ----------------------------------------------------------------------------------------
    async def scrape(self):
        """
        Scrape the directories looking for new files.
        """

        inserts = []

        # TODO: Use asyncio tasks to parellize scraping directories.
        for directory in self.__directories:
            logger.info(f"scraping {directory}")
            await self.scrape_directory(directory, inserts)

        # Flush any remaining inserts to the database.
        await self.flush_inserts(inserts)

    # ----------------------------------------------------------------------------------------
    async def scrape_directory(self, directory, inserts):
        """
        Scrape the directory looking for new files.
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
                # TODO: Use transaction to batch the insertions of collected images.
                await self.add_insert(filename, inserts)
                self.__known_filenames.append(filename)
                new_count = new_count + 1

        if new_count >= 0:
            seconds = "%0.3f" % (t1 - t0)
            logger.info(
                f"from {directory} found {new_count} files"
                f" among {len(filenames)} total files in {seconds} seconds"
            )

    # ----------------------------------------------------------------------------------------
    async def add_insert(self, filename, inserts):
        """
        Add new insert for later flush.
        """

        if len(inserts) >= 1000:
            await self.flush_inserts(inserts)

        error = None
        target_position_x = None
        target_position_y = None
        try:
            image = Image.open(filename)

            width, height = image.size
        except Exception as exception:
            error = str(exception)

            width = None
            height = None

        # Add a new insert with the fields in the proper order.
        inserts.append(
            [filename, error, width, height, target_position_x, target_position_y]
        )

    # ----------------------------------------------------------------------------------------
    async def flush_inserts(self, inserts):
        """
        Do the actual inserts by executemany.
        """

        if len(inserts) == 0:
            return

        logger.debug(f"flushing {len(inserts)} inserts")
        await self.__dataface.execute(
            f"INSERT INTO {Tablenames.CRYSTAL_WELLS}"
            f" ({CrystalWellFieldnames.FILENAME},"
            f" {CrystalWellFieldnames.ERROR},"
            f" {CrystalWellFieldnames.WIDTH},"
            f" {CrystalWellFieldnames.HEIGHT},"
            f" {CrystalWellFieldnames.TARGET_POSITION_X},"
            f" {CrystalWellFieldnames.TARGET_POSITION_Y})"
            " VALUES (?, ?, ?, ?, ?, ?)",
            subs=inserts,
        )

        inserts.clear()
