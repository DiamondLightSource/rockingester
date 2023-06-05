from pathlib import Path

from dls_utilpack.visit import get_xchem_directory

# Crystal plate constants.
from xchembku_api.crystal_plate_objects.constants import TREENODE_NAMES_TO_THING_TYPES

# Dataface client context.
from xchembku_api.models.crystal_plate_filter_model import CrystalPlateFilterModel

# Crystal well pydantic model.
from xchembku_api.models.crystal_plate_model import CrystalPlateModel

from rockingester_lib.ftrix_client import FtrixClient


class PlateInjector:
    def __init__(self, ftrix_client: FtrixClient, xchembku_client):

        self.__ftrix_client = ftrix_client
        self.__xchembku_client = xchembku_client

    # ----------------------------------------------------------------------------------------
    async def find_or_inject_barcode(
        self, barcode: str, visits_directory: str
    ) -> CrystalPlateModel:
        """
        Find barcode in xchembku database, or, if not found, add it from ftrix.

        If not in xchembku, always add barcode to xchembku, even if some kind of error to do with the plate.
        """

        # Search in xchembku for the barcode.
        crystal_plate_models = await self.__xchembku_client.fetch_crystal_plates(
            CrystalPlateFilterModel(barcode=barcode)
        )
        if len(crystal_plate_models) > 0:
            return crystal_plate_models[0]

        # Start a model object to be injected and returned.
        crystal_plate_model = CrystalPlateModel(barcode=barcode)

        # Look up the barcode in the Formulatrix database.
        record = await self.__ftrix_client.query_barcode(barcode)
        if record is None:
            crystal_plate_model.error = "barcode not found Formulatrix database"

        else:
            crystal_plate_model.formulatrix__plate__id = int(
                record["formulatrix__plate__id"]
            )
            crystal_plate_model.formulatrix__experiment__name = record[
                "formulatrix__experiment__name"
            ]

            plate_type = record["plate_type"]
            thing_type = TREENODE_NAMES_TO_THING_TYPES.get(plate_type)

            if thing_type is None:
                crystal_plate_model.error = f"unexpected plate type {plate_type}"

            else:
                crystal_plate_model.thing_type = thing_type

                # Get a proper visit name from the formulatrix's "experiment" tree_node name.
                # The techs name the experiment tree node like sw30864-12_something,
                # and the visit is parsed out as the part before the first underscore.
                try:
                    visit_directory = get_xchem_directory(
                        visits_directory,
                        crystal_plate_model.formulatrix__experiment__name,
                    )
                    # The xchem_subdirectory comes out like sw30864/sw30864-12.
                    # We only store the actual visit into the database field.
                    crystal_plate_model.visit = Path(visit_directory).name

                except Exception as exception:
                    crystal_plate_model.error = str(exception)

        # Always insert into xchembku, even if some error is on it.
        await self.__xchembku_client.upsert_crystal_plates([crystal_plate_model])

        return crystal_plate_model
