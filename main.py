#!/usr/bin/python
"""Main script for Mongodb profile example."""
import argparse
import asyncio
import logging
import os
import random
import sys
import uuid
from datetime import datetime, timezone

import motor.motor_asyncio
from pymongo import InsertOne
from pymongo.errors import PyMongoError

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

TEMPLATE_PATH = "templates"
TEMPLATE_NAME = "sample_document"

BATCH_SIZE = 10
MAX_VALUE = 100
WAIT_TIME = 10


def generate_guid():
    """Generate a UUID string."""
    return str(uuid.uuid4())


def get_date_isoformat(date):
    """Format Iso Formatted Date."""
    cur_time = date.replace(tzinfo=timezone.utc, microsecond=0)
    return cur_time.isoformat().replace("+00:00", "Z")


def get_date_now_isoformat():
    """Generate Iso Formatted Date based on Now."""
    cur_time = datetime.utcnow()
    return get_date_isoformat(cur_time)


async def create_sample_data():
    """Generate Sample Data."""
    event_id = generate_guid()
    value = random.uniform(1, MAX_VALUE)
    event_time = datetime.utcnow()

    sample_data = {
        "event_id": event_id,
        "event_desc": f"Event desc for {event_id}",
        "event_time": get_date_isoformat(event_time),
        "value": value,
    }

    return sample_data


async def run():
    """Create sample messages."""
    # print("Loading template.")
    # loader = FileSystemLoader(TEMPLATE_PATH)
    # env = Environment(loader=loader)
    # template = env.get_template(TEMPLATE_NAME)

    # set a 5-second connection timeout
    client = motor.motor_asyncio.AsyncIOMotorClient(
        DB_CONN, serverSelectionTimeoutMS=5000
    )
    try:
        print(await client.server_info())
        perf_test_db = client.perf_test_database

        # Loop Forever
        while True:

            requests = []

            for _ in range(BATCH_SIZE):
                data = await create_sample_data()
                # message = template.render_json(data)

                requests.append(InsertOne(data))

                logging.info("Sending Event %s", data)

            perf_test_db.message_collection.bulk_write(requests)
            logging.info("waiting...")
            await asyncio.sleep(WAIT_TIME)

    except PyMongoError:
        print("Unable to connect to the server.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Mongodb Profile - Event Generator.",
        add_help=True,
    )
    parser.add_argument(
        "--connection_string",
        "-c",
        help="Mongodb connection string",
    )
    args = parser.parse_args()

    DB_CONN = args.connection_string or os.environ.get("DB_CONN")

    loop = asyncio.get_event_loop()
    loop.create_task(run())
    loop.run_forever()
