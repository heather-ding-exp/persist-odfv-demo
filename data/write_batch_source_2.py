# import pandas as pd
# import numpy as np
# import sqlite3
# import petl
# from petl import look, fromdb
# from datetime import datetime
# from pandas import Timestamp

# path="transformed_customer_rating.parquet"
# d = {'hotel_id': [1001, 1002, 1003, 1004, 1005], 'customer_specific_rating_1': np.zeros(5, dtype=np.float64), 'customer_specific_rating_2': np.zeros(5, dtype=np.float64), 'event_timestamp': ["2023-05-25 09:00:00+00:00", "2023-05-25 09:00:00+00:00", "2023-05-25 09:00:00+00:00", "2023-05-25 09:00:00+00:00", "2023-05-25 09:00:00+00:00"], 'created': ["2023-06-09 10:02:08.141640", "2023-06-09 10:02:08.141640", "2023-06-09 10:02:08.141640", "2023-06-09 10:02:08.141640", "2023-06-09 10:02:08.141640"]}
# df = pd.DataFrame(data = d)
# df["event_timestamp"] = pd.to_datetime(df["event_timestamp"])
# df['created'] = pd.to_datetime(df['created'])
# df.to_parquet(path)

# print(pd.read_parquet(path))
