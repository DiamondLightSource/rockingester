# This tutorial program shows you how to update an image record.

import asyncio
import json

from xchembku_api.databases.constants import CrystalWellFieldnames
from xchembku_api.databases.constants import Types as XchembkuDatabaseTypes
from xchembku_api.datafaces.constants import Types as XchembkuDatafaceTypes
from xchembku_api.datafaces.context import Context as XchembkuContext

# Context creator.

# Specify the xchembku client type to be a local database.
client_specification = {
    "type": XchembkuDatafaceTypes.AIOSQLITE,
    "database": {
        "type": XchembkuDatabaseTypes.AIOSQLITE,
        "filename": "database/xchembku_dataface.sqlite",
    },
}


async def tutorial():
    async with XchembkuContext(client_specification) as client_interface:
        # This is the request which is sent to update the image.
        request = {
            "filename": ".*1.jpg",
            CrystalWellFieldnames.CRYSTAL_PROBABILITY: 0.9,
        }

        # Send the request to the server and get the response.
        response = await client_interface.update_crystal_well(request)

        # Show the response, which is None if success, otherwise a dict with errors in it.
        print(json.dumps(response, indent=4))


if __name__ == "__main__":
    asyncio.run(tutorial())
