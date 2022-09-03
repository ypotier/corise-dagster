from typing import List

from dagster import (
    In,
    Nothing,
    Out,
    ResourceDefinition,
    RetryPolicy,
    RunRequest,
    ScheduleDefinition,
    SkipReason,
    graph,
    op,
    sensor,
    static_partitioned_config,
)
from project.resources import mock_s3_resource, redis_resource, s3_resource
from project.sensors import get_s3_keys
from project.types import Aggregation, Stock


@op(
    config_schema={"s3_key": str},
    required_resource_keys={"s3"},
    out={"stocks": Out(dagster_type=List[Stock])},
    tags={"kind": "s3"},
    description="Get a list of stocks from an S3 file",
)
def get_s3_data(context):
    stocks = []
    for stock in context.resources.s3.get_data(context.op_config["s3_key"]):
        stocks.append(Stock.from_list(stock))
    return stocks


@op(
    ins={"stocks": In(dagster_type=List[Stock])},
    out=Out(dagster_type=Aggregation),
    tags={"kind": "python"},
    description="Get an aggregation of the highest stock",
)
def process_data(stocks: List[Stock]) -> Aggregation:
    max_stock = max(stocks, key=lambda stock: stock.high)
    return Aggregation(date=max_stock.date, high=max_stock.high)


@op(
    required_resource_keys={"redis"},
    ins={"max_stock": In(dagster_type=Aggregation)},
    out=Out(dagster_type=Nothing),
    tags={"kind": "redis"},
    description="Save data to Redis",
)
def put_redis_data(context, max_stock: Aggregation):
    context.resources.redis.put_data(str(max_stock.date), str(max_stock.high))


@graph
def week_3_pipeline():
    stocks = get_s3_data()
    my_agg = process_data(stocks)
    put_redis_data(my_agg)


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
}


docker = {
    "resources": {
        "s3": {
            "config": {
                "bucket": "dagster",
                "access_key": "test",
                "secret_key": "test",
                "endpoint_url": "http://localstack:4566",
            }
        },
        "redis": {
            "config": {
                "host": "redis",
                "port": 6379,
            }
        },
    },
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
}


def docker_config():
    pass


local_week_3_pipeline = week_3_pipeline.to_job(
    name="local_week_3_pipeline",
    config=local,
    resource_defs={
        "s3": mock_s3_resource,
        "redis": ResourceDefinition.mock_resource(),
    },
)

docker_week_3_pipeline = week_3_pipeline.to_job(
    name="docker_week_3_pipeline",
    config=docker_config,
    resource_defs={
        "s3": s3_resource,
        "redis": redis_resource,
    },
)


local_week_3_schedule = None  # Add your schedule

docker_week_3_schedule = None  # Add your schedule


@sensor
def docker_week_3_sensor():
    pass
