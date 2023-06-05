import asyncio
import logging
import os
import shutil
import time
from pathlib import Path
from typing import Dict, List

import pytds
from dls_utilpack.callsign import callsign
from dls_utilpack.explain import explain2
from dls_utilpack.require import require
from dls_utilpack.visit import (
    VisitNotFound,
    get_xchem_directory,
    get_xchem_subdirectory,
)
from PIL import Image

# Crystal plate constants.
from xchembku_api.crystal_plate_objects.constants import TREENODE_NAMES_TO_THING_TYPES

# Crystal plate object interface.
from xchembku_api.crystal_plate_objects.interface import (
    Interface as CrystalPlateInterface,
)

# Dataface client context.
from xchembku_api.datafaces.context import Context as XchembkuDatafaceClientContext
from xchembku_api.models.crystal_plate_filter_model import CrystalPlateFilterModel

# Crystal plate pydantic model.
from xchembku_api.models.crystal_plate_model import CrystalPlateModel

# Crystal well pydantic model.
from xchembku_api.models.crystal_well_model import CrystalWellModel

# Crystal plate objects factory.
from xchembku_lib.crystal_plate_objects.crystal_plate_objects import CrystalPlateObjects

# Base class for collector instances.
from rockingester_lib.collectors.base import Base as CollectorBase

logger = logging.getLogger(__name__)

thing_type = "rockingester_lib.collectors.direct_poll"


# ------------------------------------------------------------------------------------------
class DirectPoll(CollectorBase):
    """
    Object representing an image collector.
    The behavior is to start a coro task to waken every few seconds and scan for newly created plate directories.
    Image files are pushed to xchembku.
    """

    # ----------------------------------------------------------------------------------------
    def __init__(self, specification, predefined_uuid=None):
        CollectorBase.__init__(
            self, thing_type, specification, predefined_uuid=predefined_uuid
        )

        s = f"{callsign(self)} specification", self.specification()

        type_specific_tbd = require(s, self.specification(), "type_specific_tbd")

        # The sources for the collecting.
        self.__plates_directories = require(s, type_specific_tbd, "plates_directories")

        # The root directory of all visits.
        self.__visits_directory = Path(
            require(s, type_specific_tbd, "visits_directory")
        )

        # The subdirectory under a visit where to put subwell images that are collected.
        self.__visit_plates_subdirectory = Path(
            require(s, type_specific_tbd, "visit_plates_subdirectory")
        )

        # Explicit list of barcodes to process (used when testing a deployment).
        self.__ingest_only_barcodes = type_specific_tbd.get("ingest_only_barcodes")

        # Maximum time to wait for final image to arrive, relative to time of last arrived image.
        self.__max_wait_seconds = require(s, type_specific_tbd, "max_wait_seconds")

        # Database where we will get plate barcodes and add new wells.
        self.__xchembku_client_context = None
        self.__xchembku = None

        # This flag will stop the ticking async task.
        self.__keep_ticking = True
        self.__tick_future = None

        # This is the last formulatrix plate we have ingested, used to avoid re-handling the same plate.
        self.__latest_formulatrix__plate__id = 0

        # This is the list of plates indexed by their barcode.
        self.__crystal_plate_models_by_barcode: Dict[CrystalPlateModel] = {}

        # The plate names which we have already finished handling within the current instance.
        self.__handled_plate_names = []

    # ----------------------------------------------------------------------------------------
    async def activate(self) -> None:
        """
        Activate the object.

        Then it starts the coro task to awaken every few seconds to scrape the directories.
        """

        # Make the xchembku client context.
        s = require(
            f"{callsign(self)} specification",
            self.specification(),
            "type_specific_tbd",
        )
        s = require(
            f"{callsign(self)} type_specific_tbd",
            s,
            "xchembku_dataface_specification",
        )
        self.__xchembku_client_context = XchembkuDatafaceClientContext(s)

        # Activate the context.
        await self.__xchembku_client_context.aenter()

        # Get a reference to the xchembku interface provided by the context.
        self.__xchembku = self.__xchembku_client_context.get_interface()

        # Poll periodically.
        self.__tick_future = asyncio.get_event_loop().create_task(self.tick())

    # ----------------------------------------------------------------------------------------
    async def deactivate(self) -> None:
        """
        Deactivate the object.

        Causes the coro task to stop.

        This implementation then releases resources relating to the xchembku connection.
        """

        if self.__tick_future is not None:
            # Set flag to stop the periodic ticking.
            self.__keep_ticking = False
            # Wait for the ticking to stop.
            await self.__tick_future

        # Forget we have an xchembku client reference.
        self.__xchembku = None

        if self.__xchembku_client_context is not None:
            await self.__xchembku_client_context.aexit()
            self.__xchembku_client_context = None

    # ----------------------------------------------------------------------------------------
    async def tick(self) -> None:
        """
        A coro task which does periodic checking for new files in the directories.

        Stops when flag has been set by other tasks.

        # TODO: Use an event to awaken ticker early to handle stop requests sooner.
        """

        while self.__keep_ticking:
            try:
                # Fetch all the plates we don't have yet.
                await self.fetch_plates_by_barcode()

                # Scrape all the configured plates directories.
                await self.scrape()
            except Exception as exception:
                logger.error(explain2(exception, "scraping"), exc_info=exception)

            # TODO: Make periodic tick period to be configurable.
            await asyncio.sleep(1.0)

    # ----------------------------------------------------------------------------------------
    async def fetch_plates_by_barcode(self) -> None:
        """
        Fetch all barcodes in the database which we don't have in memory yet.
        """

        # Fetch all the plates we don't have yet.
        plate_models = await self.__xchembku.fetch_crystal_plates(
            CrystalPlateFilterModel(
                from_formulatrix__plate__id=self.__latest_formulatrix__plate__id
            ),
            why="[ROCKINGESTER POLL]",
        )

        # Add the plates to the list, assumed sorted by formulatrix__plate__id.
        for plate_model in plate_models:
            self.__crystal_plate_models_by_barcode[plate_model.barcode] = plate_model

            # Remember the highest one we got to shorten the query for next time.
            self.__latest_formulatrix__plate__id = plate_model.formulatrix__plate__id

    # ----------------------------------------------------------------------------------------
    async def fetch_crystal_plate_model_for_barcode(
        self,
        plate_name: str,
        plate_barcode: str,
    ) -> CrystalPlateModel:
        """
        Fetch the crystal_plate_model for plate with the given barcode.
        """

        # See if we have this barcode in the xchembku database already.
        crystal_plate_models = await self.__xchembku.fetch_crystal_plates(
            CrystalPlateFilterModel(barcode=plate_barcode),
            why="[ROCKINGESTER POLL]",
        )

        if len(crystal_plate_models) > 0:
            return crystal_plate_models[0]

        # Use ftrixminer to try to get that barcode from the Formulatrix database.
        crystal_plate_model = await self.query_formulatrix(plate_barcode)

        # This barcode is not in the database?
        if crystal_plate_model is None:
            logger.debug(
                f"[ROCKDIR] plate_barcode {plate_barcode} is not in the database"
            )
            self.__handled_plate_names.append(plate_name)
            return None

        try:
            # Try to get the visit directory from the visit stored in the database's plate record.
            visit_directory = Path(
                get_xchem_directory(
                    self.__visits_directory,
                    crystal_plate_model.visit,
                )
            )
        # This is an improperly formatted visit name?
        except ValueError:
            logger.debug(
                f"[ROCKDIR] plate_barcode {plate_barcode}"
                f" is in the database, but visit {crystal_plate_model.visit} is improperly formed"
            )
            self.__handled_plate_names.append(plate_name)
            return None
        # This visit is not found on disk?
        except VisitNotFound:
            logger.debug(
                f"[ROCKDIR] plate_barcode {plate_barcode}"
                f" is in the database, but cannot find visit directory in {self.__visits_directory}"
            )
            self.__handled_plate_names.append(plate_name)
            return None

    # ----------------------------------------------------------------------------------------
    async def scrape(self) -> None:
        """
        Scrape all the configured directories looking for new files.
        """

        # TODO: Use asyncio tasks to parellize scraping plates directories.
        for directory in self.__plates_directories:
            await self.scrape_plates_directory(Path(directory))

    # ----------------------------------------------------------------------------------------
    async def scrape_plates_directory(
        self,
        plates_directory: Path,
    ) -> None:
        """
        Scrape a single directory looking for subdirectories which correspond to plates.
        """

        if not plates_directory.is_dir():
            return

        plate_names = [
            entry.name for entry in os.scandir(plates_directory) if entry.is_dir()
        ]

        # Make sure we scrape the plate directories in barcode-order, which is the same as date order.
        plate_names.sort()

        logger.debug(
            f"[ROCKINGESTER POLL] found {len(plate_names)} plate directories in {plates_directory}"
        )

        for plate_name in plate_names:
            await self.scrape_plate_directory(plates_directory / plate_name)

    # ----------------------------------------------------------------------------------------
    async def scrape_plate_directory(
        self,
        plate_directory: Path,
    ) -> None:
        """
        Scrape a single directory looking for subdirectories which correspond to plates.
        """

        plate_name = plate_directory.name

        # We already handled this plate name?
        if plate_name in self.__handled_plate_names:
            # logger.debug(
            #     f"[ROCKINGESTER POLL] plate_barcode {plate_barcode}"
            #     f" is already handled in this instance"
            # )
            return

        # Get the plate's barcode from the directory name.
        plate_barcode = plate_name[0:4]

        # We have a specific list we want to process?
        if self.__ingest_only_barcodes is not None:
            if plate_barcode not in self.__ingest_only_barcodes:
                return

        # Get the matching plate record from the xchembku or formulatrix database.
        crystal_plate_model = await self.fetch_crystal_plate_model_for_barcode(
            plate_name,
            plate_barcode,
        )

        if crystal_plate_model.visit is not None:
            visit_directory = get_xchem_subdirectory(crystal_plate_model.visit)

            # Scrape the directory when all image files have arrived.
            await self.scrape_plate_directory_if_complete(
                plate_directory,
                crystal_plate_model,
                visit_directory,
            )

    # ----------------------------------------------------------------------------------------
    async def scrape_plate_directory_if_complete(
        self,
        plate_directory: Path,
        crystal_plate_model: CrystalPlateModel,
        visit_directory: Path,
    ) -> None:
        """
        Scrape a single directory looking for new files.

        Adds discovered files to internal list which gets pushed when it reaches a configurable size.

        TODO: Consider some other flow where well images can be copied as they arrive instead of doing them all in a bunch.
        """

        # Name of the destination directory where we will permanently store ingested well image files.
        target = (
            visit_directory / self.__visit_plates_subdirectory / plate_directory.name
        )

        # We have already put this plate directory into the visit directory?
        # This shouldn't really happen except when someone has been fiddling with the database.
        # TODO: Have a way to rebuild rockingest after database wipe, but images have already been copied to the visit.
        if target.is_dir():
            # Presumably this is done, so no error but log it.
            logger.debug(
                f"[ROCKDIR] plate_barcode {plate_directory.name} is apparently already copied to {target}"
            )
            self.__handled_plate_names.append(plate_directory.stem)
            return

        # This is the first time we have scraped a directory for this plate record in the database?
        if crystal_plate_model.rockminer_collected_stem is None:
            # Update the path stem in the crystal plate record.
            # TODO: Consider if important to report/record same barcodes on different rockmaker directories.
            crystal_plate_model.rockminer_collected_stem = plate_directory.stem
            await self.__xchembku.upsert_crystal_plates(
                [crystal_plate_model], "update rockminer_collected_stem"
            )

        # Get all the well images in the plate directory and the latest arrival time.
        subwell_names = []
        max_wait_seconds = self.__max_wait_seconds
        max_mtime = os.stat(plate_directory).st_mtime

        with os.scandir(plate_directory) as entries:
            for entry in entries:
                subwell_names.append(entry.name)
                max_mtime = max(max_mtime, entry.stat().st_mtime)

        # TODO: Verify that time.time() where rockingester runs matches os.stat() on filesystem from which images are collected.
        waited_seconds = time.time() - max_mtime

        # Make an object corresponding to the crystal plate model's type.
        crystal_plate_object = CrystalPlateObjects().build_object(
            {"type": crystal_plate_model.thing_type}
        )

        # Don't handle the plate directory until all images have arrived or some maximum wait has exceeded.
        if len(subwell_names) < crystal_plate_object.get_well_count():
            if waited_seconds < max_wait_seconds:
                logger.debug(
                    f"[PLATEWAIT] waiting longer since found only {len(subwell_names)}"
                    f" out of {crystal_plate_object.get_well_count()} subwell images"
                    f" in {plate_directory}"
                    f" after waiting {'%0.1f' % waited_seconds} out of {max_wait_seconds} seconds"
                )
                return
            else:
                logger.warning(
                    f"[PLATEDONE] done waiting even though found only {len(subwell_names)}"
                    f" out of {crystal_plate_object.get_well_count()} subwell images"
                    f" in {plate_directory}"
                    f" after waiting {'%0.1f' % waited_seconds} out of {max_wait_seconds} seconds"
                )
        else:
            logger.debug(
                f"[PLATEDONE] done waiting since found all {len(subwell_names)}"
                f" out of {crystal_plate_object.get_well_count()} subwell images"
                f" in {plate_directory}"
                f" after waiting {'%0.1f' % waited_seconds} out of {max_wait_seconds} seconds"
            )

        # Sort wells by name so that tests are deterministic.
        subwell_names.sort()

        crystal_well_models: List[CrystalWellModel] = []
        for subwell_name in subwell_names:
            # Make the well model, including image width/height.
            crystal_well_model = await self.ingest_well(
                plate_directory,
                subwell_name,
                crystal_plate_model,
                crystal_plate_object,
                target,
            )

            # Append well model to the list of all wells on the plate.
            crystal_well_models.append(crystal_well_model)

        # Here we create or update the crystal well records into xchembku.
        # TODO: Make sure that direct_poll does not double-create crystal well records if scrape is re-run with a different filename path.
        await self.__xchembku.upsert_crystal_wells(crystal_well_models)

        # Copy scraped directory to visit, replacing what might already be there.
        # TODO: Handle case where we upsert the crystal_well record but then unable to copy image file.
        shutil.copytree(
            plate_directory,
            target,
        )

        logger.info(
            f"copied {len(subwell_names)} well images from plate {plate_directory.name} to {target}"
        )

        # Remember we "handled" this one.
        self.__handled_plate_names.append(plate_directory.stem)

    # ----------------------------------------------------------------------------------------
    async def ingest_well(
        self,
        plate_directory: Path,
        subwell_name: str,
        crystal_plate_model: CrystalPlateModel,
        crystal_plate_object: CrystalPlateInterface,
        target: Path,
    ) -> CrystalWellModel:
        """
        Ingest the well into the database.

        Move the well image file to the ingested area.
        """

        input_well_filename = plate_directory / subwell_name
        ingested_well_filename = target / subwell_name

        # Stems are like "9acx_01A_1".
        # Convert the stem into a position as shown in soakdb3.
        position = crystal_plate_object.normalize_subwell_name(Path(subwell_name).stem)

        error = None
        try:
            image = Image.open(input_well_filename)
            width, height = image.size
        except Exception as exception:
            error = str(exception)
            width = None
            height = None

        crystal_well_model = CrystalWellModel(
            position=position,
            filename=str(ingested_well_filename),
            crystal_plate_uuid=crystal_plate_model.uuid,
            error=error,
            width=width,
            height=height,
        )

        return crystal_well_model

    # ----------------------------------------------------------------------------------------
    async def close_client_session(self):
        """"""

        pass

    # ----------------------------------------------------------------------------------------
    async def query_formulatrix(self, barcode: str) -> CrystalPlateModel:
        """
        Query formulatrix for plates with given barcodes.
        """

        # Query mssql or dummy.
        rows = await self.query(barcode)

        if len(rows) == 0:
            # Wrap a model around the attributes.
            crystal_plate_model = CrystalPlateModel(
                barcode=str(row[1]),
                error="barcode not found in formulatrix database",
            )

        else:
            # Loop over the rows we got back from the query.
            for row in rows:
                formulatrix__plate__id = int(row[0])
                thing_type = TREENODE_NAMES_TO_THING_TYPES.get(row[3])
                if thing_type is None:
                    raise RuntimeError(
                        f"programming error: plate type {row[3]} unexpected"
                    )

                # Get a proper visit name from the formulatrix's "experiment" tree_node name.
                # The techs name the experiment tree node like sw30864-12_something,
                # and the visit is parsed out as the part before the first underscore.
                formulatrix__experiment__name = str(row[2])
                visit = None
                error = None
                try:
                    xchem_subdirectory = get_xchem_subdirectory(
                        formulatrix__experiment__name
                    )
                    # The xchem_subdirectory comes out like sw30864/sw30864-12.
                    # We only store the actual visit into the database field.
                    visit = Path(xchem_subdirectory).name

                except Exception as exception:
                    error = str(exception)

                # Wrap a model around the attributes.
                crystal_plate_model = CrystalPlateModel(
                    visit=visit,
                    barcode=str(row[1]),
                    thing_type=thing_type,
                    formulatrix__experiment__name=formulatrix__experiment__name,
                    formulatrix__plate__id=formulatrix__plate__id,
                    error=error,
                )

            # Add plate to our database.
            # I don't worry about performance hit of adding plates one by one with upsert
            # since new plates don't get added very often.
            await self.__xchembku.upsert_crystal_plates([crystal_plate_model])

    # ----------------------------------------------------------------------------------------
    async def query(self, barcode: str) -> List[List]:
        """
        Read dummy data from configuration.
        """

        server = self.__mssql["server"]

        if server == "dummy":
            return await self.query_dummy(barcode)
        else:
            return await self.query_mssql(barcode)

    # ----------------------------------------------------------------------------------------
    async def query_mssql(self, barcode: str) -> List[List]:
        """
        Scrape discover new plates in the Formulatrix database.
        """

        # Connect to the RockMaker database at every query.
        # TODO: Use pytds connection pooling.
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
            f"\n  AND Plate.ID NOT IN ({', '.join(self.__latest_formulatrix__plate__ids)})"
        )

        cursor = connection.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()

        if self.__query_count % 60 == 0:
            logger.debug(
                f"[FTRIXMINER POLL] query #{self.__query_count}. got {len(rows)} rows from\n{sql}"
            )

        self.__query_count += 1

        return rows

    # ----------------------------------------------------------------------------------------
    async def query_dummy(self, barcodes: List[str]) -> List[List]:
        """
        Read dummy data from configuration.
        """

        database = self.__mssql["database"]
        records = self.__mssql[database]

        # Keep only records that haven't been queried before.
        new_records = []
        for record in records:
            if record[0] not in self.__latest_formulatrix__plate__ids:
                new_records.append(record)

        return new_records

    # ----------------------------------------------------------------------------------------
    async def close_client_session(self):
        """"""

        pass
