import os
import requests
import pandas
from google.transit import gtfs_realtime_pb2


API_KEY = os.environ.get("SWIFTLY_API_KEY")

today = pandas.Timestamp.today(tz="US/Pacific").date()
filename = f"trip-updates/trip-updates-{today.isoformat()}.parquet"

if os.path.exists(filename):
    print(f"Reading from {filename}")
    df = pandas.read_parquet(filename)
else:
    df = pandas.DataFrame()

response = requests.get(
    "https://api.goswift.ly/real-time/lametro/gtfs-rt-trip-updates",
    headers={"Authorization": API_KEY},
)
response.raise_for_status()

feed_rt = gtfs_realtime_pb2.FeedMessage()
feed_rt.ParseFromString(response.content)

relmap = {
    0: "scheduled",
    1: "added",
    2: "unscheduled",
    3: "canceled",
}

updates = []
for entity in feed_rt.entity:
    if entity.HasField("trip_update"):
        trip_update = entity.trip_update
        updates.append(
            {
                "trip_id": trip_update.trip.trip_id,
                "schedule_relationship": relmap[trip_update.trip.schedule_relationship],
                "timestamp": pandas.Timestamp.fromtimestamp(trip_update.timestamp),
            }
        )
print(f"Got {len(updates)} updates")

df = pandas.concat(
    [df, pandas.DataFrame.from_records(updates)],
    axis=0,
).drop_duplicates(subset="trip_id", keep="last")

print(f"Writing to {filename}")
df.to_parquet(filename)
