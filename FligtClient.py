from datetime import datetime
from typing import Union

import pandas as pd
import pyarrow as pa
import pyarrow.flight as fl
from joblib import dump, load

from sklearn.linear_model import LinearRegression

entity_df = pd.DataFrame.from_dict(
    {
        # entity's join key -> entity values
        "driver_id": [1001, 1002, 1003],
        # "event_timestamp" (reserved key) -> timestamps
        "event_timestamp": [
            datetime(2021, 4, 12, 10, 59, 42),
            datetime(2021, 4, 12, 8, 12, 10),
            datetime(2021, 4, 12, 16, 40, 26),
        ],
        # (optional) label name -> label values. Feast does not process these
        "label_driver_reported_satisfaction": [1, 5, 3],
        # values we're using for an on-demand transformation
        "val_to_add": [1, 2, 3],
        "val_to_add_2": [10, 20, 30],
    }
)

record_batch_entity = pa.Table.from_pandas(entity_df)
flight_info_entity = pa.flight.FlightDescriptor.for_command("entity_df_descriptor")


features = [
    "driver_hourly_stats:conv_rate",
    "driver_hourly_stats:acc_rate",
    "driver_hourly_stats:avg_daily_trips",
    "transformed_conv_rate:conv_rate_plus_val1",
    "transformed_conv_rate:conv_rate_plus_val2",
]

# Convert the list of features to an Arrow Array
features_array =  pa.array(features)
features_batch = pa.RecordBatch.from_arrays([features_array], ['features'])
flight_info_features = pa.flight.FlightDescriptor.for_command("features_descriptor")



client = pa.flight.connect("grpc://0.0.0.0:8815")

writer, _ = client.do_put(flight_info_entity, record_batch_entity.schema)
writer.write_table(record_batch_entity)
writer.close()

writer, _ = client.do_put(flight_info_features, features_batch.schema)
writer.write_batch(features_batch)
writer.close()



upload_descriptor = pa.flight.FlightDescriptor.for_command("entity_df_descriptor")
flight = client.get_flight_info(upload_descriptor)
descriptor = flight.descriptor

# Send request and get offline feature store
reader = client.do_get(flight.endpoints[0].ticket)
read_table = reader.read_all()
training_df= read_table.to_pandas().head()

print("----- Feature schema -----\n")
print(training_df.info())

print()
print("-----  Features -----\n")
print(training_df.head())

print('------training_df----')

print(training_df)


# Train model
target = "label_driver_reported_satisfaction"

reg = LinearRegression()
train_X = training_df[training_df.columns.drop(target).drop("event_timestamp")]
train_Y = training_df.loc[:, target]
reg.fit(train_X[sorted(train_X)], train_Y)

# Save model
dump(reg, "../feast_project/feature_repo/driver_model.bin")

print()
print("-----  Materialize the latest feature values into our online store -----\n")
print(training_df.head())

try:
    buf = pa.allocate_buffer(0)
    action = pa.flight.Action("materialize", buf)
    print('Running action materialize')
    for result in client.do_action(action):
        print("Got result", result.body.to_pybytes())
except pa.lib.ArrowIOError as e:
    print("Error calling action:", e)


