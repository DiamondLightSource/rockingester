from typing import Dict, List, Optional

import pytds
from dls_utilpack.callsign import callsign
from dls_utilpack.require import require

# Crystal plate constants.
from xchembku_api.crystal_plate_objects.constants import TREENODE_NAMES_TO_THING_TYPES


class FtrixClient:
    def __init__(self, specification: Dict):

        s = f"{callsign(self)} specification"
        self.__mssql = require(s, specification, "mssql")

    async def connect(self):
        pass

    async def disconnect(self):
        pass

    # ----------------------------------------------------------------------------------------
    async def query_barcode(self, barcode: str) -> Optional[Dict]:
        """
        Query the formulatrix database for te plate record with the given barcode.
        """

        server = self.__mssql["server"]

        if server == "dummy":
            return await self.query_barcode_dummy(barcode)
        else:
            return await self.query_barcode_mssql(barcode)

    # ----------------------------------------------------------------------------------------
    async def query_barcode_mssql(self, barcode: str) -> Optional[Dict]:
        """
        Query the MSSQL formulatrix database for te plate record with the given barcode.
        """

        # Connect to the RockMaker database at every tick.
        # TODO: Handle failure to connect to RockMaker database.
        connection = pytds.connect(
            self.__mssql["server"],
            self.__mssql["database"],
            self.__mssql["username"],
            self.__mssql["password"],
        )

        # Select only plate types we care about.
        treenode_names = [
            f"'{str(name)}'" for name in list(TREENODE_NAMES_TO_THING_TYPES.keys())
        ]

        # Plate's treenode is "ExperimentPlate".
        # Parent of ExperimentPlate is "Experiment", aka visit
        # Parent of Experiment is "Project", aka plate type.
        # Parent of Project is "ProjectsFolder", we only care about "XChem"
        # Get all xchem barcodes and the associated experiment name.
        sql = (
            "SELECT"
            "\n  Plate.ID AS id,"
            "\n  Plate.Barcode AS barcode,"
            "\n  experiment_node.Name AS experiment,"
            "\n  plate_type_node.Name AS plate_type"
            "\nFROM Plate"
            "\nJOIN Experiment ON experiment.ID = plate.experimentID"
            "\nJOIN TreeNode AS experiment_node ON experiment_node.ID = Experiment.TreeNodeID"
            "\nJOIN TreeNode AS plate_type_node ON plate_type_node.ID = experiment_node.ParentID"
            "\nJOIN TreeNode AS projects_folder_node ON projects_folder_node.ID = plate_type_node.ParentID"
            f"\nWHERE Plate.Barcode = '{barcode}'"
            "\n  AND projects_folder_node.Name = 'xchem'"
            f"\n  AND plate_type_node.Name IN ({',' .join(treenode_names)})"
        )

        cursor = connection.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()

        if len(rows) == 0:
            return None
        else:
            row = rows[0]
            record = {
                "formulatrix__plate__id": row[0],
                "barcode": row[1],
                "formulatrix__experiment__name": row[2],
                "plate_type": row[3],
            }
            return record

    # ----------------------------------------------------------------------------------------
    async def query_barcode_dummy(self, barcode: str) -> Optional[Dict]:
        """
        Query the dummy database for te plate record with the given barcode.
        """

        database = self.__mssql["database"]
        rows = self.__mssql[database]

        for row in rows:
            if row[1] == barcode:
                record = {
                    "formulatrix__plate__id": row[0],
                    "barcode": row[1],
                    "formulatrix__experiment__name": row[2],
                    "plate_type": row[3],
                }
                return record

        return None


class FtrixClientContext:
    def __init__(self, specification):
        self.__client = FtrixClient(specification)

    async def __aenter__(self):
        await self.__client.connect()
        return self.__client

    async def __aexit__(self, type, value, traceback):
        if self.__client is not None:
            await self.__client.disconnect()
            self.__client = None
