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
from xchembku_lib.datafaces.context import Context as XchembkuDatafaceServerContext

# Object able to talk to the formulatrix database.
from rockingester_lib.ftrix_client import FtrixClientContext

# Object which can inject new xchembku plate records discovered while looking in subwell images.
from rockingester_lib.plate_injector import PlateInjector

# Base class for the tester.
from tests.base import Base

logger = logging.getLogger(__name__)


# ----------------------------------------------------------------------------------------
class TestPlateInjector:
    """
    Test plate injector class.
    """

    def test(self, constants, logging_setup, output_directory):

        # Configuration file to use.
        configuration_file = "tests/configurations/direct_poll.yaml"

        PlateInjectorTester().main(constants, configuration_file, output_directory)


# ----------------------------------------------------------------------------------------
class PlateInjectorTester(Base):
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

        These are 4 barcodes.  The first matches the barcode in the database, so it gets scraped.
        The second matches no barcode in the database, so it is ignored.
        The third matches a barcode, but the visit is improperly formatted, so it is ignored.
        The fourth barcode is not scraped because it is not configured in the ingest_only_barcodes list.
        """

        # Get the multiconf from the testing configuration yaml.
        multiconf = self.get_multiconf()

        # Load the multiconf into a dict.
        multiconf_dict = await multiconf.load()

        # Reference the dict entry for the xchembku dataface.
        xchembku_dataface_specification = multiconf_dict[
            "xchembku_dataface_specification"
        ]

        # Make the xchembku server context.
        xchembku_server_context = XchembkuDatafaceServerContext(
            xchembku_dataface_specification
        )
        # Make the xchembku client context.
        xchembku_client_context = XchembkuDatafaceClientContext(
            xchembku_dataface_specification
        )

        # Reference the dict entry for the ftrix client specification.
        ftrix_client_specification = multiconf_dict["ftrix_client_specification"]

        # Use different "database" in the dummy mssql.
        ftrix_client_specification["mssql"]["database"] = "records_for_plate_injector"

        ftrix_client_context = FtrixClientContext(ftrix_client_specification)

        # Remember stuff from the multiconf so we can assert some things later.
        self.__visits_directory = Path(multiconf_dict["visits_directory"])

        # Start the client context for the remote access to the xchembku.
        async with xchembku_client_context:
            # Start the server context xchembku which starts the process.
            async with xchembku_server_context:

                async with ftrix_client_context as ftrix_client:
                    await self.__run_the_test(
                        ftrix_client, xchembku_client_context.interface
                    )

    # ----------------------------------------------------------------------------------------

    async def __run_the_test(self, ftrix_client, xchembku_client):
        """ """

        plate_injector = PlateInjector(ftrix_client, xchembku_client)

        # ----------------------------
        barcode = "98ax"
        crytal_plate_model = await plate_injector.find_or_inject_barcode(
            barcode, self.__visits_directory
        )

        assert crytal_plate_model.barcode == barcode
        assert "does not conform" in crytal_plate_model.error

        # ----------------------------
        barcode = "zzaa"
        crytal_plate_model = await plate_injector.find_or_inject_barcode(
            barcode, self.__visits_directory
        )

        assert crytal_plate_model.barcode == barcode
        assert "not found" in crytal_plate_model.error

        # Now we should find it in the xchembku database.
        crytal_plate_model2 = await plate_injector.find_or_inject_barcode(
            barcode, self.__visits_directory
        )
        assert crytal_plate_model.uuid == crytal_plate_model2.uuid

        # ----------------------------

        # Make a directory satisfy barcode 98ab.
        existing_visit_directory = self.__visits_directory / get_xchem_subdirectory(
            "cm00001-1"
        )
        existing_visit_directory.mkdir(parents=True)

        barcode = "98ab"
        crytal_plate_model = await plate_injector.find_or_inject_barcode(
            barcode, self.__visits_directory
        )

        assert crytal_plate_model.barcode == barcode
        assert crytal_plate_model.error is None
