# Use standard logging in this module.
import logging

# Types.
from rockingester_api.collectors.constants import Types

# Exceptions.
from rockingester_api.exceptions import NotFound

# Class managing list of things.
from rockingester_api.things import Things

logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------------------
__default_rockingester_collector = None


def rockingester_collectors_set_default(rockingester_collector):
    global __default_rockingester_collector
    __default_rockingester_collector = rockingester_collector


def rockingester_collectors_get_default():
    global __default_rockingester_collector
    if __default_rockingester_collector is None:
        raise RuntimeError("rockingester_collectors_get_default instance is None")
    return __default_rockingester_collector


# -----------------------------------------------------------------------------------------


class Collectors(Things):
    """
    List of available rockingester_collectors.
    """

    # ----------------------------------------------------------------------------------------
    def __init__(self, name=None):
        Things.__init__(self, name)

    # ----------------------------------------------------------------------------------------
    def build_object(self, specification):
        """"""

        rockingester_collector_class = self.lookup_class(specification["type"])

        try:
            rockingester_collector_object = rockingester_collector_class(specification)
        except Exception as exception:
            raise RuntimeError(
                "unable to build rockingester collector object for type %s"
                % (rockingester_collector_class)
            ) from exception

        return rockingester_collector_object

    # ----------------------------------------------------------------------------------------
    def lookup_class(self, class_type):
        """"""

        if class_type == Types.AIOHTTP:
            from rockingester_api.collectors.aiohttp import Aiohttp

            return Aiohttp

        if class_type == Types.DIRECT:
            from rockingester_lib.collectors.direct_poll import DirectPoll

            return DirectPoll

        raise NotFound(
            f"unable to get rockingester collector class for type {class_type}"
        )
