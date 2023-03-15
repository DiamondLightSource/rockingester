import asyncio
import logging
import os
import time

# Things xchembku provides.
from xchembku_api.datafaces.context import Context as XchembkuDatafaceClientContext
from xchembku_api.datafaces.datafaces import xchembku_datafaces_get_default

# Client context creator.
from rockingest_api.collectors.context import Context as CollectorClientContext

# Server context creator.
from rockingest_lib.collectors.context import Context as CollectorServerContext

# Base class for the tester.
from tests.base import Base

logger = logging.getLogger(__name__)


# ----------------------------------------------------------------------------------------
class TestCollectorDirectPoll:
    """
    Test collector interface by direct call.
    """

    def test(self, constants, logging_setup, output_directory):

        # Configuration file to use.
        configuration_file = "tests/configurations/direct_poll.yaml"

        CollectorTester().main(constants, configuration_file, output_directory)


# ----------------------------------------------------------------------------------------
class TestCollectorService:
    """
    Test collector interface through network interface.
    """

    def test(self, constants, logging_setup, output_directory):

        # Configuration file to use.
        configuration_file = "tests/configurations/service.yaml"

        CollectorTester().main(constants, configuration_file, output_directory)


# ----------------------------------------------------------------------------------------
class CollectorTester(Base):
    """
    Test scraper collector's ability to automatically discover files and push them to xchembku.
    """

    # ----------------------------------------------------------------------------------------
    async def _main_coroutine(self, constants, output_directory):
        """ """

        # Get the multiconf from the testing configuration yaml.
        multiconf = self.get_multiconf()

        # Load the multiconf into a dict.
        multiconf_dict = await multiconf.load()

        # Reference the dict entry for the xchembku dataface.
        xchembku_dataface_specification = multiconf_dict[
            "xchembku_dataface_specification"
        ]

        # Make the xchembku client context, expected to be direct (no server).
        xchembku_client_context = XchembkuDatafaceClientContext(
            xchembku_dataface_specification
        )

        collector_specification = multiconf_dict["rockingest_collector_specification"]
        # Make the server context.
        collector_server_context = CollectorServerContext(collector_specification)

        # Make the client context.
        collector_client_context = CollectorClientContext(collector_specification)

        image_count = 2

        # Start the client context for the direct access to the xchembku.
        async with xchembku_client_context:
            # Start the matching xchembku client context.
            async with collector_client_context:
                # Start the collector server context.
                async with collector_server_context:
                    await self.__run_part1(image_count, constants, output_directory)

                logger.debug(
                    "------------ restarting collector server --------------------"
                )

                # Start the server again.
                # This covers the case where collector starts by finding existing entries in the database and doesn't double-collect those on disk.
                async with collector_server_context:
                    await self.__run_part2(image_count, constants, output_directory)

    # ----------------------------------------------------------------------------------------

    async def __run_part1(self, image_count, constants, output_directory):
        """ """
        # Reference the xchembku object which the context has set up as the default.
        xchembku = xchembku_datafaces_get_default()

        # Make the scrapable directory.
        images_directory = f"{output_directory}/images"
        os.makedirs(images_directory)

        # Get list of images before we create any of the scrape-able files.
        records = await xchembku.fetch_crystal_wells_filenames()

        assert len(records) == 0, "images before any scraping"

        # Create a few scrape-able files.
        for i in range(10, 10 + image_count):
            filename = f"{images_directory}/%06d.jpg" % (i)
            with open(filename, "w") as stream:
                stream.write("")

        # Wait for all the images to appear.
        time0 = time.time()
        timeout = 5.0
        while True:

            # Get all images.
            records = await xchembku.fetch_crystal_wells_filenames()

            # Stop looping when we got the images we expect.
            if len(records) >= image_count:
                break

            if time.time() - time0 > timeout:
                raise RuntimeError(
                    f"only {len(records)} images out of {image_count}"
                    f" registered within {timeout} seconds"
                )
            await asyncio.sleep(1.0)

        # Wait a couple more seconds to make sure there are no extra images appearing.
        await asyncio.sleep(2.0)
        # Get all images.
        records = await xchembku.fetch_crystal_wells_filenames()

        assert len(records) == image_count, "images after scraping"

    # ----------------------------------------------------------------------------------------

    async def __run_part2(self, image_count, constants, output_directory):
        """ """
        # Reference the xchembku object which the context has set up as the default.
        xchembku = xchembku_datafaces_get_default()

        await asyncio.sleep(2.0)
        # Get all images after servers start up and run briefly.
        records = await xchembku.fetch_crystal_wells_filenames()

        assert len(records) == image_count, "images after restarting scraper"
