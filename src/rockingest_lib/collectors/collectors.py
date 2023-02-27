# Use standard logging in this module.
import logging

# Class managing list of things.
from dls_utilpack.things import Things

from rockingest_lib.collectors.constants import Types

# Exceptions.
from rockingest_lib.exceptions import NotFound

logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------------------
__default_collector = None


def collectors_set_default(collector):
    global __default_collector
    __default_collector = collector


def collectors_get_default():
    global __default_collector
    if __default_collector is None:
        raise RuntimeError("collectors_get_default instance is None")
    return __default_collector


class Collectors(Things):
    """
    List of available collectors.
    """

    # ----------------------------------------------------------------------------------------
    def __init__(self, name="collectors"):
        Things.__init__(self, name)

    # ----------------------------------------------------------------------------------------
    def build_object(self, specification, predefined_uuid=None):
        """"""

        collector_class = self.lookup_class(specification["type"])

        try:
            collector_object = collector_class(
                specification, predefined_uuid=predefined_uuid
            )
        except Exception as exception:
            raise RuntimeError(
                "unable to build collector object of class %s"
                % (collector_class.__name__)
            ) from exception

        return collector_object

    # ----------------------------------------------------------------------------------------
    def lookup_class(self, class_type):
        """"""

        if class_type == Types.AIOHTTP:
            from rockingest_lib.collectors.aiohttp import Aiohttp

            return Aiohttp

        elif class_type == Types.SCRAPE:
            from rockingest_lib.collectors.scrape import Scrape

            return Scrape

        else:
            try:
                RuntimeClass = Things.lookup_class(self, class_type)
                return RuntimeClass
            except NotFound:
                raise NotFound("unable to get collector class for %s" % (class_type))
