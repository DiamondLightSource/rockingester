import asyncio
import logging
import os
import time

# Things xchembku provides.
from xchembku_api.datafaces.datafaces import xchembku_datafaces_get_default
from xchembku_lib.datafaces.context import Context as XchembkuDatafaceContext

# Context creator.
from rockingest_lib.collectors.context import Context as CollectorContext

# Base class for the tester.
from tests.base import Base

logger = logging.getLogger(__name__)


# ----------------------------------------------------------------------------------------
class TestCollectorScraper:
    def test(self, constants, logging_setup, output_directory):
        """
        Test scraper collector's ability to automatically discover files and push them to xchembku.
        """

        # Configuration file to use.
        configuration_file = "tests/configurations/direct.yaml"
        # The collector configuration to replace in the configuration file for this test.
        actual_collspec = "rockingest_specification_scraper"

        ScraperTester(
            actual_collspec=actual_collspec,
        ).main(constants, configuration_file, output_directory)


# ----------------------------------------------------------------------------------------
class ScraperTester(Base):
    """
    Class to test the collector.
    """

    def __init__(
        self,
        actual_collspec=None,
    ):
        Base.__init__(self)

        self.__actual_collspec = actual_collspec

    async def _main_coroutine(self, constants, output_directory):
        """ """

        # Write some scrape-able files.
        scan_directory = f"{output_directory}/scan_directory"
        os.makedirs(scan_directory)

        # Load the configuration.
        multiconf = self.get_multiconf()
        context_configuration = await multiconf.load()

        # Make an xchembku context.
        xchembku_context = XchembkuDatafaceContext(
            context_configuration["xchembku_dataface_specification"]
        )

        # Refer to the desired actual collector specification.
        actual_collspec = context_configuration[self.__actual_collspec]

        actual_collspec_tbd = actual_collspec["type_specific_tbd"]
        actual_collspec_tbd["glob"] = f"{scan_directory}/*.jpg"

        server_collspec = context_configuration["rockingest_specification"]
        server_collspec["type_specific_tbd"][
            "actual_rockingest_specification"
        ] = actual_collspec

        rockingest_context = CollectorContext(
            context_configuration["rockingest_specification"]
        )

        image_count = 2

        # Start the xchembku server.
        async with xchembku_context:

            # Start the rockingest server.
            async with rockingest_context:
                # Wait long enough for the collector to activate and start ticking.
                await asyncio.sleep(2.0)

                # Get all images before we create any of the scrape-able files.
                records = (
                    await xchembku_datafaces_get_default().fetch_rockingest_images()
                )

                if len(records) != 0:
                    raise RuntimeError(f"found {len(records)} images but expected 0")

                # Create a few scrape-able files.
                # These will be picked up by the scraper's tick coroutine.
                for i in range(10, 10 + image_count):
                    filename = f"{scan_directory}/%06d.jpg" % (i)
                    with open(filename, "w") as stream:
                        stream.write("")

                # Wait for all the images to appear.
                time0 = time.time()
                timeout = 5.0
                while True:

                    # Get all images.
                    records = (
                        await xchembku_datafaces_get_default().fetch_rockingest_images()
                    )

                    if len(records) >= image_count:
                        break

                    if time.time() - time0 > timeout:
                        raise RuntimeError(
                            f"image not registered within {timeout} seconds"
                        )
                    await asyncio.sleep(1.0)

                # Wait a couple more seconds to make sure there are no extra images appearing.
                await asyncio.sleep(2.0)
                # Get all images.
                records = (
                    await xchembku_datafaces_get_default().fetch_rockingest_images()
                )

                if len(records) != image_count:
                    raise RuntimeError(
                        f"found {len(records)} images but expected {image_count}"
                    )

            logger.debug("------------ restarting collector --------------------")
            # Start the servers again.
            # This covers the case where collector starts by finding existing entries in the database.
            async with rockingest_context:
                await asyncio.sleep(2.0)
                # Get all images after servers start up and run briefly.
                records = (
                    await xchembku_datafaces_get_default().fetch_rockingest_images()
                )

                if len(records) != image_count:
                    raise RuntimeError(
                        f"found {len(records)} images but expected {image_count}"
                    )
