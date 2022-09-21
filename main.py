#!/usr/bin/python
"""Main script for Mongodb profile example."""
import argparse
import asyncio
import logging
import math
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


async def generate_customer():
    """Generate Sample Customer."""
    max_credit_limit = 10000
    customer_id = generate_guid()
    event_time = datetime.utcnow()

    sample_data = {
        "id": customer_id,
        "name": f"Customer {customer_id}",
        "credit_limit": round(random.uniform(0, max_credit_limit)),
        "create_date": get_date_isoformat(event_time)
    }

    return sample_data

async def generate_order(customers):
    """Generate Sample Order."""
    max_order_id = 300
    min_order_id = 200

    max_quantity = 50

    max_price = 500
    price_rate = 0.046
    price_midpoint = 270

    order_id = generate_guid()
    customer_id = random.choice(customers)
    product_id = round(random.uniform(min_order_id, max_order_id))
    unit_price =  round(max_price / (1 + math.e**((-1 * price_rate)*(product_id - price_midpoint))), 2)
    quantity = round(random.uniform(1, max_quantity))

    order_value = quantity * unit_price
    event_time = datetime.utcnow()

    sample_data = {
        "order_id": order_id,
        "product_id": product_id,
        "quantity": quantity,
        "unit_cost": round(unit_price,2),
        "order_value": round(order_value,2),
        "customer_id": customer_id,
        "order_date": get_date_isoformat(event_time)
    }

    return sample_data

async def save_new_customer(db, customer):
    """Create a new Customer."""
    customer_id = db.customer_collection.insert_one(customer).inserted_id
    return customer_id


async def get_all_customers(db):
    """Get all Customers."""
    return db.customer_collection.find()

async def get_customer(db, customer_id):
    """Get a Customers."""
    return db.customer_collection.find_one({"customer_id": customer_id})

async def update_customer(db, customer_id):
    """Update a Customers."""
    new_limit = round(random.uniform(2000, 10000)),
    db.customer_collection.update_one({"customer_id": customer_id}, {"credit_limit": new_limit})
    return new_limit

async def create_new_order(db, customer):
    """Create a new Customer Order."""
    order = generate_order(customer)
    db.order_collection.insert_one(order)

async def get_customer_orders(db, customer_id):
    """Get Customer Orders."""
    return db.order_collection.find({"customer_id": customer_id})

async def remove_customer_orders(db, customer_id):
    """Remove Customer Orders."""
    db.order_collection.delete_many({"customer_id": customer_id})


async def run():
    """Create sample messages."""
    # print("Loading template.")
    # loader = FileSystemLoader(TEMPLATE_PATH)
    # env = Environment(loader=loader)
    # template = env.get_template(TEMPLATE_NAME)
    customers = {}

    # set a 5-second connection timeout
    client = motor.motor_asyncio.AsyncIOMotorClient(
        DB_CONN, serverSelectionTimeoutMS=5000
    )
    try:
        print(await client.server_info())
        perf_test_db = client.perf_test_database

        # Create a few customers
        for _ in range(10):
            customer = generate_customer()
            customer_id = save_new_customer(perf_test_db, customer)
            customers[customer_id] = customer

        # Loop Forever
        while True:

            requests = []

            # make a few orders
            # for _ in range(BATCH_SIZE):

                # Pick a customer

                # Generate an order


                # data = await create_sample_data()
                # # message = template.render_json(data)

                # requests.append(InsertOne(data))

                # logging.info("Sending Event %s", data)

            # perf_test_db.message_collection.bulk_write(requests)
            # logging.info("waiting...")
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
