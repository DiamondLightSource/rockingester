import asyncio
import logging
import time
from pathlib import Path

from dls_utilpack.visit import get_xchem_subdirectory

# Types which the CrystalPlateObjects factory can use to build an instance.
from xchembku_api.crystal_plate_objects.constants import (
    ThingTypes as CrystalPlateObjectThingTypes,
)

# Things xchembku provides.
from xchembku_api.datafaces.context import Context as XchembkuDatafaceClientContext
from xchembku_api.datafaces.datafaces import xchembku_datafaces_get_default
from xchembku_api.models.crystal_plate_filter_model import CrystalPlateFilterModel
from xchembku_api.models.crystal_plate_model import CrystalPlateModel

# Client context creator.
from rockingester_api.collectors.context import Context as CollectorClientContext

# Server context creator.
from rockingester_lib.collectors.context import Context as CollectorServerContext

# Base class for the tester.
from tests.base import Base

logger = logging.getLogger(__name__)


# ----------------------------------------------------------------------------------------
class TestPlatewaitDirectPoll:
    """
    Test collector interface by direct call.
    """

    def test(self, constants, logging_setup, output_directory):

        # Configuration file to use.
        configuration_file = "tests/configurations/direct_poll.yaml"

        PlatewaitTester().main(constants, configuration_file, output_directory)


# ----------------------------------------------------------------------------------------
class TestPlatewaitService:
    """
    Test collector interface through network interface.
    """

    def test(self, constants, logging_setup, output_directory):

        # Configuration file to use.
        configuration_file = "tests/configurations/service.yaml"

        PlatewaitTester().main(constants, configuration_file, output_directory)


# ----------------------------------------------------------------------------------------
class PlatewaitTester(Base):
    """
    Test scraper collector's ability to automatically discover files and push them to xchembku.
    """

    # ----------------------------------------------------------------------------------------
    async def _main_coroutine(self, constants, output_directory):
        """
        This tests the collector behavior when the images are slow in coming.
        """

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

        collector_specification = multiconf_dict["rockingester_collector_specification"]
        # Make the server context.
        collector_server_context = CollectorServerContext(collector_specification)

        # Make the client context.
        collector_client_context = CollectorClientContext(collector_specification)

        # Remember the collector specification so we can assert some things later.
        self.__visits_directory = Path(multiconf_dict["visits_directory"])
        self.__visit_plates_subdirectory = Path(
            multiconf_dict["visit_plates_subdirectory"]
        )

        scrapable_image_count = 4

        # Start the client context for the direct access to the xchembku.
        async with xchembku_client_context:
            # Start the collector client context.
            async with collector_client_context:
                # And the collector server context which starts the coro.
                async with collector_server_context:
                    await self.__run_the_test(
                        scrapable_image_count, constants, output_directory
                    )

    # ----------------------------------------------------------------------------------------

    async def __run_the_test(self, scrapable_image_count, constants, output_directory):
        """ """
        # Reference the xchembku object which the context has set up as the default.
        xchembku = xchembku_datafaces_get_default()

        # Make the plate on which the wells reside.
        visit = "cm00001-1_otherstuff"
        created_crystal_plate_models = []

        scrabable_barcode = "98ab"
        created_crystal_plate_models.append(
            CrystalPlateModel(
                formulatrix__plate__id=10,
                barcode=scrabable_barcode,
                visit=visit,
                thing_type=CrystalPlateObjectThingTypes.SWISS3,
            )
        )

        await xchembku.upsert_crystal_plates(created_crystal_plate_models)

        visit_directory = self.__visits_directory / get_xchem_subdirectory(visit)
        visit_directory.mkdir(parents=True)
        rockingester_directory = visit_directory / self.__visit_plates_subdirectory

        # Source directory which gets scraped for plates.
        plates_directory = Path(output_directory) / "SubwellImages"

        # Make the scrapable directory with some files, fewer than the total.
        # This one gets scraped as normal.
        plate_directory1 = plates_directory / "98ab_2023-04-06_RI1000-0276-3drop"
        plate_directory1.mkdir(parents=True)
        for i in range(scrapable_image_count):
            filename = plate_directory1 / self.__subwell_filename(scrabable_barcode, i)
            with open(filename, "w") as stream:
                stream.write("")

        # Wait for all the images to appear.
        time0 = time.time()
        timeout = 5.0
        while True:

            # Get all images.
            crystal_well_models = await xchembku.fetch_crystal_wells_filenames()

            # Stop looping when we got the images we expect.
            if len(crystal_well_models) >= scrapable_image_count:
                break

            if time.time() - time0 > timeout:
                raise RuntimeError(
                    f"only {len(crystal_well_models)} images out of {scrapable_image_count}"
                    f" registered within {timeout} seconds"
                )
            await asyncio.sleep(1.0)

        # Make sure the crystal plate record got its collector stem recorded.
        crystal_plate_models = await xchembku.fetch_crystal_plates(
            CrystalPlateFilterModel()
        )
        assert crystal_plate_models[0].rockminer_collected_stem == plate_directory1.stem

        # Get all images in the database.
        crystal_well_models = await xchembku.fetch_crystal_wells_filenames()
        assert (
            len(crystal_well_models) == scrapable_image_count
        ), "images after scraping"

        # Make sure the positions got recorded right in the wells.
        assert crystal_well_models[0].position == "A01a"
        assert crystal_well_models[-1].position == "A02a"

        # The first "scrapable" plate directory should still exist.
        count = sum(1 for _ in plate_directory1.glob("*") if _.is_file())
        assert count == scrapable_image_count, "first (scrapable) plate_directory"

        # We should have ingested the first barcode.
        count = sum(1 for _ in rockingester_directory.glob("*") if _.is_dir())
        assert count == 1, f"rockingester_directory {str(rockingester_directory)}"
        count = sum(
            1
            for _ in (rockingester_directory / plate_directory1.name).glob("*")
            if _.is_file()
        )
        assert (
            count == scrapable_image_count
        ), f"ingested_directory images {str(rockingester_directory)}"

    # ----------------------------------------------------------------------------------------

    def __subwell_filename(self, barcode, index):
        """
        Make a subwell image name which can be parsed by swiss3.
        """

        well_letters = "ABCDEFGH"

        well = int(index / 3)
        subwell = index % 3 + 1
        row = well_letters[int(well / 12)]
        col = "%02d" % (well % 12 + 1)

        subwell_filename = f"{barcode}_{col}{row}_{subwell}"

        return subwell_filename
