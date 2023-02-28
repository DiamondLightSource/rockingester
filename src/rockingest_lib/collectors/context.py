import logging

from rockingest_api.collectors.context import Context as CollectorContext

# Things created in the context.
from rockingest_lib.collectors.collectors import Collectors, collectors_set_default

# Base class for an asyncio context
from rockingest_lib.contexts.base import Base as ContextBase

logger = logging.getLogger(__name__)


thing_type = "rockingest_lib.rockingest_collectors.context"


class Context(ContextBase):
    """
    Asyncio context for a rockingest_dataface server object.
    On entering, it creates the object according to the specification (a dict).
    If configured, it starts the server as a coroutine, thread or process.
    On exiting, it commands the server to shut down.

    The enter and exit methods are exposed for use during testing.
    """

    # ----------------------------------------------------------------------------------------
    def __init__(self, specification):
        ContextBase.__init__(self, thing_type, specification)
        self.__api_context = None

    # ----------------------------------------------------------------------------------------
    async def aenter(self):
        """ """

        # Build the object according to the specification.
        self.server = Collectors().build_object(self.specification())

        # If there is more than one collector, the last one defined will be the default.
        collectors_set_default(self.server)

        if self.context_specification.get("start_as") == "coro":
            await self.server.activate_coro()

        elif self.context_specification.get("start_as") == "thread":
            await self.server.start_thread()

        elif self.context_specification.get("start_as") == "process":
            await self.server.start_process()

        self.__api_context = CollectorContext(self.specification())
        await self.__api_context.aenter()

    # ----------------------------------------------------------------------------------------
    async def aexit(self):
        """ """

        if self.server is not None:
            if self.context_specification.get("start_as") == "process":
                # Put in request to shutdown the server.
                await self.server.client_shutdown()

            if self.context_specification.get("start_as") == "coro":
                await self.server.direct_shutdown()

        if self.__api_context is not None:
            await self.__api_context.aexit()

        # Clear the global variable.  Important between pytests.
        collectors_set_default(None)
