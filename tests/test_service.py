import asyncio
import logging
import os
import time

# Things xchembku provides.
from xchembku_api.datafaces.datafaces import xchembku_datafaces_get_default
from xchembku_lib.datafaces.context import Context as XchembkuDatafaceContext

from rockingest_lib.collectors.collectors import collectors_get_default

# Context creator.
from rockingest_lib.collectors.context import Context as CollectorContext

# Base class for the tester.
from tests.base import Base

logger = logging.getLogger(__name__)


# ----------------------------------------------------------------------------------------
class TestService:
    def test(self, constants, logging_setup, output_directory):
        """
        Test scraper collector's ability to automatically discover files and push them to xchembku.
        """

        # Configuration file to use.
        configuration_file = "tests/configurations/service.yaml"

        ServiceTester().main(constants, configuration_file, output_directory)


# ----------------------------------------------------------------------------------------
class ServiceTester(Base):
    """
    Class to test the collector by direct call.
    """

    # ----------------------------------------------------------------------------------------
    async def _main_coroutine(self, constants, output_directory):
        """ """

        # Load the configuration.
        multiconf = self.get_multiconf()
        configuration = await multiconf.load()

        # Make a context to access the xchembku.
        xchembku_context = XchembkuDatafaceContext(
            configuration["xchembku_dataface_specification"]
        )

        # Make a context to access the xchembku.
        async with xchembku_context:
            # For short, the same singleton the service will use.
            xchembku = xchembku_datafaces_get_default()

            # Make the scrapable directory.
            images_directory = f"{output_directory}/images"
            os.makedirs(images_directory)

            rockingest_context = CollectorContext(
                configuration["rockingest_collector_specification"]
            )

            image_count = 2

            # Start the rockingest server.
            async with rockingest_context:
                # Wait long enough for the collector to activate and start ticking.
                await asyncio.sleep(2.0)

                # Get all images before we create any of the scrape-able files.
                records = await xchembku.fetch_crystal_wells_filenames()

                if len(records) != 0:
                    raise RuntimeError(f"found {len(records)} images but expected 0")

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
                            f"only {len(records)} images out of {image_count} registered within {timeout} seconds"
                        )
                    await asyncio.sleep(1.0)

                # Wait a couple more seconds to make sure there are no extra images appearing.
                await asyncio.sleep(2.0)
                # Get all images.
                records = await xchembku.fetch_crystal_wells_filenames()

                if len(records) != image_count:
                    raise RuntimeError(
                        f"found {len(records)} images but expected {image_count}"
                    )

            logger.debug("------------ restarting collector --------------------")
            # Start the servers again.
            # This covers the case where collector starts by finding existing entries in the database and doesn't double-collect those on disk.
            async with rockingest_context:
                await asyncio.sleep(2.0)
                # Get all images after servers start up and run briefly.
                records = await xchembku.fetch_crystal_wells_filenames()

                if len(records) != image_count:
                    raise RuntimeError(
                        f"found {len(records)} images but expected {image_count}"
                    )
