import asyncio
import logging
import time
from pathlib import Path

from dls_utilpack.visit import get_xchem_subdirectory

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
        """
        This tests the collector behavior.

        First it starts the xchembku and collector and loads a single plate barcode into the database.

        Then, while the collector thread is running, it creates some scrapable images.

        The test then waits a while for the scraping to be done, and verifies the outputs.

        These are in 3 barcodes.  The first matches the barcode in the database, so it gets scraped.
        The second matches no barcode in the database, so it is moved to the nobarcode area and not added to the database.
        The third barcode is not scraped because it is not configured in the ingest_only_barcodes list.
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
        self.__nobarcode_directory = Path(multiconf_dict["nobarcode_directory"])
        self.__novisit_directory = Path(multiconf_dict["novisit_directory"])

        scrapable_image_count = 288

        # Start the client context for the direct access to the xchembku.
        async with xchembku_client_context:
            # Start the collector client context.
            async with collector_client_context:
                # And the collector server context which starts the coro.
                async with collector_server_context:
                    await self.__run_part1(
                        scrapable_image_count, constants, output_directory
                    )

                logger.debug(
                    "------------ restarting collector server --------------------"
                )

                # Start the server again.
                # This covers the case where collector starts by finding existing entries in the database and doesn't double-collect those on disk.
                async with collector_server_context:
                    await self.__run_part2(
                        scrapable_image_count, constants, output_directory
                    )

    # ----------------------------------------------------------------------------------------

    async def __run_part1(self, scrapable_image_count, constants, output_directory):
        """ """
        # Reference the xchembku object which the context has set up as the default.
        xchembku = xchembku_datafaces_get_default()

        # Make the plate on which the wells reside.
        visit = "cm00001-1_otherstuff"
        created_crystal_plate_models = []
        created_crystal_plate_models.append(
            CrystalPlateModel(
                formulatrix__plate__id=10,
                barcode="98ab",
                visit=visit,
            )
        )

        nobarcode_barcode = "98ac"

        # Create a crystal plate model with a good barcode but bad visit.
        novisit_barcode = "98ad"
        created_crystal_plate_models.append(
            CrystalPlateModel(
                formulatrix__plate__id=11,
                barcode=novisit_barcode,
                visit=("X" + visit),
            )
        )

        excluded_barcode = "98ae"

        await xchembku.upsert_crystal_plates(created_crystal_plate_models)

        visit_directory = self.__visits_directory / get_xchem_subdirectory(visit)
        visit_directory.mkdir(parents=True)
        rockingester_directory = visit_directory / self.__visit_plates_subdirectory

        # Get list of images before we create any of the scrape-able files.
        crystal_well_models = await xchembku.fetch_crystal_wells_filenames()

        assert (
            len(crystal_well_models) == 0
        ), "images before any new files are put in scrapable"

        plates_directory = Path(output_directory) / "SubwellImages"

        # Make the scrapable directory with some files.
        # This one gets scraped as normal.
        plate_directory1 = plates_directory / "98ab_2023-04-06_RI1000-0276-3drop"
        plate_directory1.mkdir(parents=True)
        for i in range(10, 10 + scrapable_image_count):
            filename = plate_directory1 / ("98ab_%03dA_1.jpg" % (i))
            with open(filename, "w") as stream:
                stream.write("")

        # Make another scrapable directory with a different barcode.
        # This one gets moved into nobarcode since it doesn't match any plate.
        plate_directory2 = (
            plates_directory / f"{nobarcode_barcode}_2023-04-06_RI1000-0276-3drop"
        )
        plate_directory2.mkdir(parents=True)
        nobarcode_image_count = 3
        for i in range(10, 10 + nobarcode_image_count):
            filename = plate_directory2 / ("%s_%03dA_1.jpg" % (nobarcode_barcode, i))
            with open(filename, "w") as stream:
                stream.write("")

        # Make yet another scrapable directory with a different barcode.
        # This one gets moved into novisit since it matches a plate with a bad visit name.
        plate_directory3 = (
            plates_directory / f"{novisit_barcode}_2023-04-06_RI1000-0276-3drop"
        )
        plate_directory3.mkdir(parents=True)
        novisit_image_count = 6
        for i in range(10, 10 + novisit_image_count):
            filename = plate_directory3 / ("%s_%03dA_1.jpg" % (novisit_barcode, i))
            with open(filename, "w") as stream:
                stream.write("")

        # Make yet another scrapable directory with a different barcode.
        # This one gets completely ignored because it's not in the explicit list.
        plate_directory4 = (
            plates_directory / f"{excluded_barcode}_2023-04-06_RI1000-0276-3drop"
        )
        plate_directory4.mkdir(parents=True)
        excluded_image_count = 2
        for i in range(10, 10 + excluded_image_count):
            filename = plate_directory4 / ("%s_%03dA_1.jpg" % (excluded_barcode, i))
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

        # Wait a couple more seconds to make sure there are no extra images appearing.
        await asyncio.sleep(2.0)

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
        i = 10
        for crystal_well_model in crystal_well_models:
            assert crystal_well_model.position == "%03dA1" % (i)
            i += 1

        # The first "scrapable" plate directory should still exist.
        count = sum(1 for _ in plate_directory1.glob("*") if _.is_file())
        assert count == scrapable_image_count, "first (scrapable) plate_directory"

        # The second "nobarcode" plate directory should no longer exist.
        count = sum(1 for _ in plate_directory2.glob("*") if _.is_file())
        assert count == 0, "second plate_directory"

        # The third plate directory (novisit) is left intact.
        # We keep "novisit" plate directories for now, since Texrank still needs them.
        count = sum(1 for _ in plate_directory3.glob("*") if _.is_file())
        assert count == novisit_image_count, "third plate_directory"

        # The fourth plate directory is left intact.
        count = sum(1 for _ in plate_directory4.glob("*") if _.is_file())
        assert count == excluded_image_count, "fourth plate_directory"

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

        # We should have sent the second barcode to the nobarcode area.
        count = sum(1 for _ in self.__nobarcode_directory.glob("*") if _.is_dir())
        assert count == 1, f"nobarcode_directory {str(self.__nobarcode_directory)}"
        count = sum(
            1
            for _ in (self.__nobarcode_directory / plate_directory2.name).glob("*")
            if _.is_file()
        )
        assert (
            count == nobarcode_image_count
        ), f"nobarcode_directory images {str(self.__nobarcode_directory)}"

        # We should NOT have sent the third (novisit) barcode to the novisit area.
        count = sum(1 for _ in self.__novisit_directory.glob("*") if _.is_dir())
        assert count == 0, f"novisit_directory {str(self.__novisit_directory)}"
        count = sum(
            1
            for _ in (self.__novisit_directory / plate_directory3.name).glob("*")
            if _.is_file()
        )
        assert count == 0, f"novisit_directory images {str(self.__novisit_directory)}"

    # ----------------------------------------------------------------------------------------

    async def __run_part2(self, scrapable_image_count, constants, output_directory):
        """ """
        # Reference the xchembku object which the context has set up as the default.
        xchembku = xchembku_datafaces_get_default()

        await asyncio.sleep(2.0)
        # Get all images after servers start up and run briefly.
        records = await xchembku.fetch_crystal_wells_filenames()

        assert len(records) == scrapable_image_count, "images after restarting scraper"
