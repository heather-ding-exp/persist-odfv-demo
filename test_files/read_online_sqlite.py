# Prints all entries stored in Sqlite online store
# import pandas as pd
# import sqlite3

# # Path to the online store
# path_online = "../data/online_store.db"
# db = sqlite3.connect(path_online)
# cursor = db.cursor()
# cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
# tables = cursor.fetchall()
# for table_name in tables:
#     print(table_name)
#     table_name = table_name[0]
#     table = pd.read_sql_query("SELECT * from %s" % table_name, db)
#     print(table)
# cursor.close()
# db.close()