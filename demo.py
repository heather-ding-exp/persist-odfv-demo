import subprocess
from datetime import datetime

import pandas as pd

from feast import FeatureStore
from feast.data_source import PushMode
import feast.types as types


from threading import Thread
import time 
import concurrent.futures


def run_demo():
    store = FeatureStore(repo_path=".")
    print("\n--- Run feast apply ---")
    subprocess.run(["feast", "apply"])

    print("\n--- Historical features for training ---")
    fetch_historical_features_entity_df(store, source = "on-demand")

    print("\n--- Load features into online store ---")
    #store.materialize( start_date = datetime.now(), end_date = datetime.now())
    store.materialize( start_date = datetime.strptime("2021-04-07T00:00:00Z", "%Y-%m-%dT%H:%M:%SZ"), end_date = datetime.strptime("2023-05-08T10:02:08Z", "%Y-%m-%dT%H:%M:%SZ"))

    print("\n--- Online features + Push On Demand Features to Online Store ---")
    fetch_online_features(store, source = "first_user_input")

    print("\n--- Online features retrieved---")
    fetch_online_features(store, source="stored")

    print("\n--- Online features + Push On Demand Features to Offline Store ---")
    fetch_online_features(store, source = "second_user_input")

    print("\n--- Offline store features ---")
    fetch_historical_features_entity_df(store, source="stored")

    print("\n--- Run feast teardown ---")
    subprocess.run(["feast", "teardown"])


def fetch_historical_features_entity_df(store: FeatureStore, source: str = "on-demand"):
    # Note: see https://docs.feast.dev/getting-started/concepts/feature-retrieval for more details on how to retrieve
    # for all entities in the offline store instead
    # For batch scoring, we want the latest timestamps

    if source == "on-demand":
        entity_df = pd.DataFrame.from_dict(
                {
                    # entity's join key -> entity values
                    "hotel_id": [1001, 1002],
                    # "event_timestamp" (reserved key) -> timestamps
                    "event_timestamp": [
                        datetime(2021, 4, 12, 10, 59, 42),
                        datetime(2021, 4, 12, 8, 12, 10),
                    ],
                    # (optional) label name -> label values. Feast does not process these
                    #"label_driver_reported_satisfaction": [1, 5, 3],
                    # values we're using for an on-demand transformation
                    "customer_inp_1": [1, 2],
                    "customer_inp_2": [10, 20],
                }
            )
        features=[
            "hotel_hourly_stats:customer_rating",
            "transformed_customer_rating:customer_specific_rating_1",
            "transformed_customer_rating:customer_specific_rating_2",
        ]
    elif source == "stored":
        entity_df = pd.DataFrame.from_dict(
                {
                    # entity's join key -> entity values
                    "hotel_id": [1001, 1002],
                    # "event_timestamp" (reserved key) -> timestamps
                    "event_timestamp": [
                        datetime(2021, 4, 12, 10, 59, 42),
                        datetime(2021, 4, 12, 8, 12, 10),
                    ],
                    # (optional) label name -> label values. Feast does not process these
                
                    # no values for on-demand transformation
                }
            )
        features=[
            "hotel_hourly_stats:customer_rating",
            "transformed_customer_rating_fv:customer_specific_rating_1",
            "transformed_customer_rating_fv:customer_specific_rating_2",
        ]
        
    training_df = store.get_historical_features(
        entity_df,
        features,
    ).to_df()
    print(training_df.head())


def fetch_online_features(store, source: str = ""):
    # Retrieve persisted ODFV
    if source == "stored":
        features_to_fetch = [
            "hotel_hourly_stats:customer_rating",
            "transformed_customer_rating_fv:customer_specific_rating_1",
            "transformed_customer_rating_fv:customer_specific_rating_2",
        ]
        entity_rows = [
                {
                    "hotel_id": 1001,
                },
                {
                    "hotel_id": 1002,
                },
            ]
        t1 = time.perf_counter()
        returned_features = store.get_online_features(
            features=features_to_fetch,
            entity_rows=entity_rows,
        ).to_dict()
        for key, value in sorted(returned_features.items()):
            print(key, " : ", value)
        t2 = time.perf_counter()
        print(f'Finished in {t2 - t1} seconds')
        return

    # Compute new on-demand features and persist to online store
    else:
        features_to_fetch = [
            "hotel_hourly_stats:customer_rating",
            "transformed_customer_rating:customer_specific_rating_1",
            "transformed_customer_rating:customer_specific_rating_2",
        ]
        if source == "first_user_input":
            entity_rows = [
                {
                    "hotel_id": 1001,
                    "customer_inp_1": 30,
                    "customer_inp_2": 40,
                },
                {
                    "hotel_id": 1002,
                    "customer_inp_1": 300,
                    "customer_inp_2": 400,
                },
            ]
        elif source == "second_user_input":
            entity_rows = [
                {
                    "hotel_id": 1001,
                    "customer_inp_1": 100,
                    "customer_inp_2": 200,
                },
                {
                    "hotel_id": 1002,
                    "customer_inp_1": 1000,
                    "customer_inp_2": 2000,
                },
            ]
        t1 = time.perf_counter()
        returned_features = store.get_online_features(
            features=features_to_fetch,
            entity_rows=entity_rows,
        ).to_dict()
        
        for key, value in sorted(returned_features.items()):
            print(key, " : ", value)


        t2 = time.perf_counter()
        print(f'Finished in {t2 - t1} seconds')


        # Push proper format df to the online store
        t1 = time.perf_counter()
        df = pd.DataFrame.from_dict(returned_features)
        df = df.drop(columns = ["customer_rating"])     # Drop needed to push to the offline store, extra col is fine for push to online

        #df = df.drop(columns = ["conv_rate_plus_val1"])        # Dropping too many columns causes errors when pushing
        df["event_timestamp"] = pd.to_datetime([datetime(2021, 4, 12, 10, 59, 42), datetime(2021, 4, 12, 8, 12, 10)])
        df["created"] = pd.to_datetime([datetime.now() for row in range(len(df.index))])
        if source == "first_user_input":
            store.push("transformed_customer_rating_push_source", df, to=PushMode.ONLINE)
        elif source == "second_user_input":
            store.push("transformed_customer_rating_push_source", df, to=PushMode.OFFLINE)

        t2 = time.perf_counter()
        print(f'Persisting finished in {t2 - t1} seconds')
        return

if __name__ == "__main__":
    run_demo()
