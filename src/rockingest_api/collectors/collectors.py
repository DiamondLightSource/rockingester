# Use standard logging in this module.
import logging

# Types.
from rockingest_api.collectors.constants import Types

# Exceptions.
from rockingest_api.exceptions import NotFound

# Class managing list of things.
from rockingest_api.things import Things

logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------------------
__default_rockingest_collector = None


def rockingest_collectors_set_default(rockingest_collector):
    global __default_rockingest_collector
    __default_rockingest_collector = rockingest_collector


def rockingest_collectors_get_default():
    global __default_rockingest_collector
    if __default_rockingest_collector is None:
        raise RuntimeError("rockingest_collectors_get_default instance is None")
    return __default_rockingest_collector


# -----------------------------------------------------------------------------------------


class Collectors(Things):
    """
    List of available rockingest_collectors.
    """

    # ----------------------------------------------------------------------------------------
    def __init__(self, name=None):
        Things.__init__(self, name)

    # ----------------------------------------------------------------------------------------
    def build_object(self, specification):
        """"""

        rockingest_collector_class = self.lookup_class(specification["type"])

        try:
            rockingest_collector_object = rockingest_collector_class(specification)
        except Exception as exception:
            raise RuntimeError(
                "unable to build rockingest_collector object for type %s"
                % (rockingest_collector_class)
            ) from exception

        return rockingest_collector_object

    # ----------------------------------------------------------------------------------------
    def lookup_class(self, class_type):
        """"""

        if class_type == Types.AIOHTTP:
            from rockingest_api.collectors.aiohttp import Aiohttp

            return Aiohttp

        if class_type == Types.DIRECT:
            from rockingest_lib.collectors.direct_poll import DirectPoll

            return DirectPoll

        raise NotFound(
            "unable to get rockingest_collector class for type %s" % (class_type)
        )
