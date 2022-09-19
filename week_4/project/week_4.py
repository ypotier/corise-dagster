from typing import List

from dagster import Nothing, asset, with_resources
from project.resources import redis_resource, s3_resource
from project.types import Aggregation, Stock


@asset(
    group_name="corise",
    config_schema={"s3_key": str},
    required_resource_keys={"s3"},
    op_tags={"kind": "s3"},
    description="Get a list of stocks from an S3 file",
)
def get_s3_data(context)-> List[Stock]:
    output = list()
    for row in context.resources.s3.get_data(context.op_config["s3_key"]):
        print(row)
        stock = Stock.from_list(row)
        output.append(stock)
    return output


@asset
def process_data():
    # Use your op logic from week 3 (you will need to make a slight change)
    pass


@asset
def put_redis_data():
    # Use your op logic from week 3 (you will need to make a slight change)
    pass


get_s3_data_docker, process_data_docker, put_redis_data_docker = with_resources()
