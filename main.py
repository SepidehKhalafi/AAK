
import pandas
import requests
import datetime
import psycopg2
import sqlalchemy
from sklearn.preprocessing import OneHotEncoder


# data acquisition
csv_log = pandas.read_csv("file_path") # loading a csv file
api_log = requests.get("url_address").text # collecting data from an online source which needs to be transformed
database_log = pandas.read_sql("table", psycopg2.connect("configuration")) # loading data from a daatbase

# JSON to CSV which is either srtuctured or unstructured
def online_reading():
    try:
        with open(api_log, encoding = “utf-8”) as inputfile:
            data = pandas.read_json(inputfile)
            data.to_csv(“csvfile.csv”, encoding = “utf-8”, index = False)
    except:
        with open(api_log, encoding = “utf-8”) as inputfile:
              data = json.loads(inputfile.read())
              data = pandas.json_normalize(data)
              data.to_csv(“csvfile.csv”, encoding = “utf-8”, index = False)
    return data

api_data = online_reading()

# SQL to CSV
database_data = database_log.to_csv()

# unifying the data
final_data = pandas.concat([csv_log, api_data, database_data])

# Data cleaning and preprocessing

# cinsidering we have such a dataset to clean
data = {"employee_id": [101, 102, 101, 103, 101],
        "event_type": ["keyboard",
                        "mouse_click",
                        "keyboard",
                        "task_switch",
                        "keyboard"],
        "timestamp": ["2025-02-27 09:01:23",
                      "2025-02-27 09:02:45",
                      "INVALID_DATE",
                      "2025-02-27 09:05:10",
                      "2025-02-27 09:06:15"],
        "details": [{"key": 'A'},
                    {'button': 'left'},
                    {'key': 'Enter'},
                    {'task': 'Data Cleaning'},
                    {'key': 'Backspace'}]}

dataset = pandas.DataFrame(data)

dataset.info() # gives us an overview on data, null values and counts

threshold = pandas.Timestamp.now()
# for item in dataset["timestamp"]:
#         if item > threshold:
dataset.loc[dataset["timestamp"] > threshold, "timstamp"] = None # replacing invalid dates with null
# if we know the start date, we can add another condition for it here
dataset.loc[dataset["timestamp"] == "INVALID_DATE", "timstamp"] = None
pandas.to_datetime(dataset["timestamp"]) # to be able to use the details of it
dataset["timestamp"].fillna(method = "pad", inplace = True) # pad uses previous value as replacement

# separating and saving the details as different features
# dataset["key_pressed"] = dataset["details"].keys
# dataset["task_name"] = dataset["details"].values
ordered_keys = []
ordered_values = []
for dict_ in dataset['details']:
      for key, value in dict_.items():
        ordered_keys.append(key)
        ordered_values.append(value)    

dataset["key_pressed"] = ordered_keys
dataset["task_name"] = ordered_values

# filtering specific events
events = dataset["event_type"].filter(items = ["keyboard", "task_switch"])

# categoricl to numerical fro model training purpose
enc = OneHotEncoder(handle_unknown = "ignore")
enc.fit_transform(dataset)

# the above lines would be applied to the final_data when we have the datasets
# save to PostgreSQL
engine = sqlalchemy.create_engine("destination_url_address") # establishing the connection
final_data.to_sql("table", engine, if_exists = "append") # adding new data to existing table
