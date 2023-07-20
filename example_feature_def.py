# This is an example feature definition file
from datetime import timedelta

import pandas as pd

from feast import (
    Entity,
    FeatureService,
    FeatureView,
    Field,
    FileSource,
    PushSource,
    RequestSource,
)
from feast.on_demand_feature_view import on_demand_feature_view
from feast.types import Float32, Float64, Int64

# Define an entity for each hotel. You can think of an entity as a primary key used to
# fetch features.
hotel = Entity(name="hotel", join_keys=["hotel_id"])

# Read data from parquet files. Parquet is convenient for local development mode. For
# production, you can use your favorite DWH, such as BigQuery. See Feast documentation
# for more info.
hotel_stats_source = FileSource(
    name="hotel_hourly_stats_source",
    path="/Users/hding/Desktop/push_demo/feature_repo/data/hotel_stats.parquet",    # Change this to your current path
    timestamp_field="event_timestamp",
    created_timestamp_column="created",
)

# Our parquet files contain sample data that includes a hotel_id column, timestamps and
# three feature column. Here we define a Feature View that will allow us to serve this
# data to our model online.
hotel_stats_fv = FeatureView(
    # The unique name of this feature view. Two feature views in a single
    # project cannot have the same name
    name="hotel_hourly_stats",
    entities=[hotel],
    ttl=timedelta(days=1),
    # The list of features defined below act as a schema to both define features
    # for both materialization of features into a store, and are used as references
    # during retrieval for building a training dataset or serving features
    schema=[
        Field(name="customer_rating", dtype=Float32),
        Field(name="price_rating", dtype=Float32),
        Field(name="avg_daily_volume", dtype=Int64, description="Average daily click volume"),
    ],
    online=True,
    source=hotel_stats_source,
    # Tags are user defined key/value pairs that are attached to each
    # feature view
    tags={"team": "hotel_ranking"},
)

# Define a request data source which encodes features / information only
# available at request time (e.g. part of the user initiated HTTP request)
input_request = RequestSource(
    name="customer_inp",
    schema=[
        Field(name="customer_inp_1", dtype=Int64),
        Field(name="customer_inp_2", dtype=Int64),
    ],
)
# This defines the registration required to persist on-demand computed features
transformed_customer_rating_source = FileSource(
    name="transformed_customer_rating_source",
    path="/Users/hding/Desktop/push_demo/feature_repo/data/transformed_customer_rating.parquet",
    timestamp_field="event_timestamp",
    created_timestamp_column="created",
)

# Define an on demand feature view which can generate new features based on
# existing feature views and RequestSource features
@on_demand_feature_view(
    sources=[hotel_stats_fv, input_request],
    schema=[
        Field(name="customer_specific_rating_1", dtype=Float64),
        Field(name="customer_specific_rating_2", dtype=Float64),
    ],
    # entities = [hotel],
    # feature_view_name = "transformed_customer_rating_fv",
    # push_source_name = "transformed_customer_rating_ps",
    # batch_source = transformed_customer_rating_source
)
def transformed_customer_rating(inputs: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame()
    df["customer_specific_rating_1"] = inputs["customer_rating"] + inputs["customer_inp_1"]
    df["customer_specific_rating_2"] = inputs["customer_rating"] + inputs["customer_inp_2"]
    return df



transformed_customer_rating_push_source = PushSource(
    name= "transformed_customer_rating_push_source",
    batch_source=  hotel_stats_source #transformed_customer_rating_source,
)

transformed_customer_rating_fv = FeatureView(
    name="transformed_customer_rating_fv",
    entities=[hotel],
    ttl=timedelta(days=1),
    schema=[
        Field(name="customer_specific_rating_1", dtype=Float32),
        Field(name="customer_specific_rating_2", dtype=Float32),
    ],
    online=True,
    source=transformed_customer_rating_push_source,  
)