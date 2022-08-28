from typing import List

from dagster import In, Nothing, Out, ResourceDefinition, graph, op
from dagster_ucr.project.types import Aggregation, Stock
from dagster_ucr.resources import mock_s3_resource, redis_resource, s3_resource


@op(
    config_schema={"s3_key": str},
    out={"stocks": Out(dagster_type=List[Stock])},
    required_resource_keys={"s3"},
    tags={"kind": "s3"},
    description="Get a list of stocks from an S3 file",
)
def get_s3_data(context):
    s3 = context.resources.s3
    stocks = s3.get_data(context.op_config["s3_key"])
    return [Stock.from_list(stock) for stock in stocks]


@op(
    ins={"stocks": In(dagster_type=List[Stock])},
    out={"aggregation": Out(dagster_type=Aggregation)},
    tags={"kind": "python"},
    description="Take the list of stocks and determine the Stock with the greatest high value",
)
def process_data(stocks):
    #Stock(date=datetime(2022, 1, 1, 0, 0), close=7.0, volume=12, open=7.0, high=10.0, low=6.0)
    max_stock = sorted(stocks, key=lambda x: x.high, reverse=True)[0]
    return Aggregation(date=max_stock.date, high=max_stock.high)


@op(
    ins={"aggregation": In(dagster_type=Aggregation)},
    required_resource_keys={"redis"},
    tags={"kind": "redis"},
    description="Save data to Redis",
)
def put_redis_data(context, my_agg):
    redis = context.resources.redis
    redis.put_data(str(my_agg.date), str(my_agg.high))


@graph
def week_2_pipeline():
    stocks = get_s3_data()
    my_agg = process_data(stocks)
    put_redis_data(my_agg)


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock.csv"}}},
}

docker = {
    "resources": {
        "s3": {
            "config": {
                "bucket": "dagster",
                "access_key": "test",
                "secret_key": "test",
                "endpoint_url": "http://host.docker.internal:4566",
            }
        },
        "redis": {
            "config": {
                "host": "redis",
                "port": 6379,
            }
        },
    },
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock.csv"}}},
}

local_week_2_pipeline = week_2_pipeline.to_job(
    name="local_week_2_pipeline",
    config=local,
    resource_defs={"s3": mock_s3_resource, "redis": ResourceDefinition.mock_resource()},
)

docker_week_2_pipeline = week_2_pipeline.to_job(
    name="docker_week_2_pipeline",
    config=docker,
    resource_defs={"s3": s3_resource, "redis": redis_resource},
)
