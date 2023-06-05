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

# Client context creator.
from rockingester_api.collectors.context import Context as CollectorClientContext

# Server context creator.
from rockingester_lib.collectors.context import Context as CollectorServerContext
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
        configuration_file = "tests/configurations/direct_poll.yaml"

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
        assert record["formulatrix__experiment__name"] == "cm00001-1_something#else"

        record = await ftrix_client.query_barcode("zz00")
        assert record is None
