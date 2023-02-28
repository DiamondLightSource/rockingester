import logging
import os

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
class TestDirect:
    def test(self, constants, logging_setup, output_directory):
        """
        Test scraper collector's ability to automatically discover files and push them to xchembku.
        """

        # Configuration file to use.
        configuration_file = "tests/configurations/direct.yaml"

        DirectTester().main(constants, configuration_file, output_directory)


# ----------------------------------------------------------------------------------------
class DirectTester(Base):
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
            filters = []

            # Start the rockingest server.
            async with rockingest_context:
                await collectors_get_default().scrape()

                # Get all images before we create any of the scrape-able files.
                records = await xchembku.fetch_crystal_wells_filenames()

                if len(records) != 0:
                    raise RuntimeError(f"found {len(records)} images but expected 0")

                # Create a few scrape-able files.
                for i in range(10, 10 + image_count):
                    filename = f"{images_directory}/%06d.jpg" % (i)
                    with open(filename, "w") as stream:
                        stream.write("")

                # Get all images.
                await collectors_get_default().scrape()
                records = await xchembku.fetch_crystal_wells_filenames()

                if len(records) != image_count:
                    raise RuntimeError(
                        f"found {len(records)} images but expected {image_count}"
                    )
