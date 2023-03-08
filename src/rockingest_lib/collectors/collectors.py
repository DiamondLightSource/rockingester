# Use standard logging in this module.
import logging
from typing import Any, Dict, Optional, Type

# Class managing list of things.
from dls_utilpack.things import Things

from rockingest_lib.collectors.constants import Types

# Exceptions.
from rockingest_lib.exceptions import NotFound

logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------------------
# Use proper Singleton pattern for global instance of default_collector.
__default_collector = None


# Set the global instance of the default collector.
def collectors_set_default(collector):
    global __default_collector
    __default_collector = collector


# Get the global instance of the default collector.
def collectors_get_default():
    global __default_collector
    if __default_collector is None:
        raise RuntimeError("collectors_get_default instance is None")
    return __default_collector


class Collectors(Things):
    """
    Factory to construct a collector from a specification dict.
    """

    # ----------------------------------------------------------------------------------------
    def __init__(self, name: str = "collectors"):
        """
        Constructor.

        Args:
            name (str, optional): name of the list,
                typically only needed in debugging when there are potentially
                multiple of these. Defaults to "collectors".
        """
        Things.__init__(self, name)

    # ----------------------------------------------------------------------------------------
    def build_object(
        self,
        specification: Dict,
        predefined_uuid: Optional[str],
    ) -> Any:
        """
        Construct an object from the given specification.

        Args:
            specification (Dict): specification of the object to be constructed.
                The object's type is expected to be a string value in specification["type"].
            predefined_uuid (Optional[str], optional): uuid if known beforehand. Defaults to None,
                in which case the object will get a newly generated uuid.

        Raises:
            NotFound: If no matching class can be found.
            RuntimeError: When a class is found, but the object cannot be constructed due to some error.

        Returns:
            object: A constructed object of the desired class type.
        """

        # Get a class object from the string name in the specification.
        # TODO:
        collector_class = self.lookup_class(specification["type"])

        try:
            # Instantiate the class from the specification dict.
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
    def lookup_class(self, class_type: str) -> Type:
        """
        Return a class object given its string type.

        Args:
            class_type (str): type of object.  Can be a well-known short name
                or a Python class name or filename::classname

        Raises:
            NotFound: If no matching class can be found.

        Returns:
            class: a class object (not an implementation).
        """

        # TODO: Use ABC to declare classes as interfaces with abstract methods.

        if class_type == Types.AIOHTTP:
            from rockingest_lib.collectors.aiohttp import Aiohttp

            return Aiohttp

        elif class_type == Types.DIRECT:
            from rockingest_lib.collectors.direct import Direct

            return Direct

        # Not the nickname of a class type?
        else:
            try:
                # Presume this is a python class name or filename::classname.
                RuntimeClass = Things.lookup_class(self, class_type)
                return RuntimeClass
            except NotFound:
                raise NotFound("unable to get collector class for %s" % (class_type))
