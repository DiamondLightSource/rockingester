import logging

from rockingester_lib.ftrix_client import FtrixClientContext

# Base class for the tester.
from tests.base import Base

logger = logging.getLogger(__name__)


# ----------------------------------------------------------------------------------------
class TestFtrixClient:
    """
    The ftrix client class.
    """

    def test(self, constants, logging_setup, output_directory):

        # Configuration file to use.
        configuration_file = "tests/configurations/direct_sqlite.yaml"

        FtrixClientTester().main(constants, configuration_file, output_directory)


# ----------------------------------------------------------------------------------------
class FtrixClientTester(Base):
    """
    Test client to Formulatrix client.
    """

    # ----------------------------------------------------------------------------------------
    async def _main_coroutine(self, constants, output_directory):
        """ """

        # Get the multiconf from the testing configuration yaml.
        multiconf = self.get_multiconf()

        # Load the multiconf into a dict.
        multiconf_dict = await multiconf.load()

        # Reference the dict entry for the ftrix client specification.
        ftrix_client_specification = multiconf_dict["ftrix_client_specification"]

        ftrix_client_context = FtrixClientContext(ftrix_client_specification)

        async with ftrix_client_context as ftrix_client:
            await self.__run_the_test(ftrix_client)

    # ----------------------------------------------------------------------------------------

    async def __run_the_test(self, ftrix_client):
        """ """

        record = await ftrix_client.query_barcode("98ab")
        assert record["formulatrix__experiment__name"] == "cm00001-1_scrapable"

        record = await ftrix_client.query_barcode("zz00")
        assert record is None
