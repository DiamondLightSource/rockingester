import logging
import multiprocessing
import threading

# Utilities.
from dls_utilpack.callsign import callsign
from dls_utilpack.require import require

# Base class which maps flask tasks to methods.
from dls_utilpack.thing import Thing

# Base class for an aiohttp server.
from rockingest_lib.base_aiohttp import BaseAiohttp

# Factory to make a Collector.
from rockingest_lib.collectors.collectors import Collectors

# Collector protocolj things.
from rockingest_lib.collectors.constants import Commands, Keywords

logger = logging.getLogger(__name__)

thing_type = "rockingest_lib.collectors.aiohttp"


# ------------------------------------------------------------------------------------------
class Aiohttp(Thing, BaseAiohttp):
    """
    Object representing a collector which receives tasks from aiohttp.
    """

    # ----------------------------------------------------------------------------------------
    def __init__(self, specification=None, predefined_uuid=None):
        Thing.__init__(self, thing_type, specification, predefined_uuid=predefined_uuid)
        BaseAiohttp.__init__(
            self, specification["type_specific_tbd"]["aiohttp_specification"]
        )

        self.__actual_collector = None

    # ----------------------------------------------------------------------------------------
    def callsign(self):
        """"""
        return "%s %s" % ("Collector.Aiohttp", BaseAiohttp.callsign(self))

    # ----------------------------------------------------------------------------------------
    def activate_process(self):
        """"""

        try:
            multiprocessing.current_process().name = "collector"

            self.activate_process_base()

        except Exception as exception:
            logger.exception("exception in collector process", exc_info=exception)

        logger.debug(f"[PIDAL] {callsign(self)} is returning from activate_process")

    # ----------------------------------------------------------------------------------------
    def activate_thread(self, loop):
        """
        Called from inside a newly created thread.
        """

        try:
            threading.current_thread().name = "collector"

            self.activate_thread_base(loop)

        except Exception as exception:
            logger.exception(
                f"unable to start {callsign(self)} thread", exc_info=exception
            )

    # ----------------------------------------------------------------------------------------
    async def activate_coro(self):
        """"""
        try:
            # Build a local collector for our back-end.
            self.__actual_collector = Collectors().build_object(
                self.specification()["type_specific_tbd"][
                    "actual_collector_specification"
                ]
            )

            logger.info("[COLSHUT] calling self.__actual_collector.activate()")
            # Get the local implementation started.
            await self.__actual_collector.activate()

            # ----------------------------------------------
            logger.info("[COLSHUT] calling BaseAiohttp.activate_coro_base(self)")
            await BaseAiohttp.activate_coro_base(self)

            logger.info("[COLSHUT] returning")

        except Exception as exception:
            raise RuntimeError(
                "exception while starting collector server"
            ) from exception

    # ----------------------------------------------------------------------------------------
    async def direct_shutdown(self):
        """"""

        logger.info(
            f"[COLSHUT] in direct_shutdown self.__actual_collector is {self.__actual_collector}"
        )

        # ----------------------------------------------
        if self.__actual_collector is not None:
            # Disconnect our local dataface connection, i.e. the one which holds the database connection.
            logger.info("[COLSHUT] awaiting self.__actual_collector.deactivate()")
            await self.__actual_collector.deactivate()
            logger.info(
                "[COLSHUT] got return from self.__actual_collector.deactivate()"
            )

        # ----------------------------------------------
        # Let the base class stop the server listener.
        await self.base_direct_shutdown()

    # ----------------------------------------------------------------------------------------
    # From http client, request server to submit task for execution.

    # async def fire(self, message):
    #     """"""
    #     # Build a local collector for our client side.
    #     actual_collector = Collectors().build_object(
    #         self.specification()["type_specific_tbd"][
    #             "actual_collector_specification"
    #         ]
    #     )

    #     logger.debug(f"[DMOTF] firing actual {callsign(actual_collector)}")
    #     await actual_collector.fire(message)
    #     logger.debug("[DMOTF] firing complete")

    # ----------------------------------------------------------------------------------------
    async def fire(self, message):
        """"""
        return await self.__send_protocolj("fire", message)

    # ----------------------------------------------------------------------------------------
    async def __send_protocolj(self, function, *args, **kwargs):
        """"""

        return await self.client_protocolj(
            {
                Keywords.COMMAND: Commands.EXECUTE,
                Keywords.PAYLOAD: {
                    "function": function,
                    "args": args,
                    "kwargs": kwargs,
                },
            },
        )

    # ----------------------------------------------------------------------------------------
    async def __do_locally(self, function, args, kwargs):
        """"""

        # logger.info(describe("function", function))
        # logger.info(describe("args", args))
        # logger.info(describe("kwargs", kwargs))

        function = getattr(self.__actual_collector, function)

        response = await function(*args, **kwargs)

        return response

    # ----------------------------------------------------------------------------------------
    async def dispatch(self, request_dict, opaque):
        """"""

        command = require("request json", request_dict, Keywords.COMMAND)

        if command == Commands.EXECUTE:
            payload = require("request json", request_dict, Keywords.PAYLOAD)
            response = await self.__do_locally(
                payload["function"], payload["args"], payload["kwargs"]
            )
        else:
            raise RuntimeError("invalid command %s" % (command))

        return response
