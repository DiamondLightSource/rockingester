# Use standard logging in this module.
import logging

# Types.
from rockingest_api.datafaces.constants import Types

# Exceptions.
from rockingest_api.exceptions import NotFound

# Class managing list of things.
from rockingest_api.things import Things

logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------------------
__default_rockingest_dataface = None


def rockingest_datafaces_set_default(rockingest_dataface):
    global __default_rockingest_dataface
    __default_rockingest_dataface = rockingest_dataface


def rockingest_datafaces_get_default():
    global __default_rockingest_dataface
    if __default_rockingest_dataface is None:
        raise RuntimeError("rockingest_datafaces_get_default instance is None")
    return __default_rockingest_dataface


# -----------------------------------------------------------------------------------------


class Datafaces(Things):
    """
    List of available rockingest_datafaces.
    """

    # ----------------------------------------------------------------------------------------
    def __init__(self, name=None):
        Things.__init__(self, name)

    # ----------------------------------------------------------------------------------------
    def build_object(self, specification):
        """"""

        rockingest_dataface_class = self.lookup_class(specification["type"])

        try:
            rockingest_dataface_object = rockingest_dataface_class(specification)
        except Exception as exception:
            raise RuntimeError(
                "unable to build rockingest_dataface object for type %s"
                % (rockingest_dataface_class)
            ) from exception

        return rockingest_dataface_object

    # ----------------------------------------------------------------------------------------
    def lookup_class(self, class_type):
        """"""

        if class_type == Types.AIOHTTP:
            from rockingest_api.datafaces.aiohttp import Aiohttp

            return Aiohttp

        if class_type == Types.DIRECT:
            from rockingest_lib.datafaces.direct import Direct

            return Direct

        raise NotFound(
            "unable to get rockingest_dataface class for type %s" % (class_type)
        )
