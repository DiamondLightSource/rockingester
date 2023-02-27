import asyncio

# Use standard logging in this module.
import logging

# Base class for cli subcommands.
from rockingest_cli.subcommands.base import ArgKeywords, Base

# Context creator.
from rockingest_lib.collectors.context import Context

logger = logging.getLogger()


# --------------------------------------------------------------
class Start(Base):
    """
    Start one or more services and keep them running until ^C.
    """

    def __init__(self, args, mainiac):
        super().__init__(args)

    # ----------------------------------------------------------------------------------------
    def run(self):
        """ """

        # Run in asyncio event loop.
        asyncio.run(self.__run_coro())

    # ----------------------------------------------------------
    async def __run_coro(self):
        """"""

        # Load the configuration.
        rockingest_multiconf = self.get_multiconf(vars(self._args))
        configuration = await rockingest_multiconf.load()

        # Make a service context from the specification in the configuration.
        context = Context(configuration["rockingest_dataface_specification"])

        # Open the context which starts the service process.
        async with context:
            try:
                while True:
                    await asyncio.sleep(1.0)
                    if not await context.is_process_started():
                        logger.info("process is not started")
                        break
                    if not await context.is_process_alive():
                        logger.info("process is not alive")
                        break
            except KeyboardInterrupt:
                pass

    # ----------------------------------------------------------
    def add_arguments(parser):

        parser.add_argument(
            "--configuration",
            "-c",
            help="Configuration file.",
            type=str,
            metavar="yaml filename",
            default=None,
            dest=ArgKeywords.CONFIGURATION,
        )

        return parser
