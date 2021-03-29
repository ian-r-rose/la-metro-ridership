import sys

import pandas


fname = sys.argv[1]

# Load in the data
ridership = pandas.read_parquet(fname)
ridership = ridership.rename(columns={"Estimated Ridership": "ridership"})

# Select weekday ridershpi
weekday_ridership = ridership[ridership["Day Type"] == "Weekday"]

# Get yearly averages for each line so we can compare line ridership to
# baseline values from previous years
yearly_averages = (
    weekday_ridership
    .groupby(["year", "line"])
    ["ridership"]
    .mean()
    .sort_values(ascending=False)
    .to_frame()
)
average_ridership_2019 = (
    yearly_averages
    .loc[("2019", slice(None))]
    .sort_values("ridership", ascending=False)
)
average_ridership_2018 = (
    yearly_averages
    .loc[("2018", slice(None))]
    .sort_values("ridership", ascending=False)
)

# We will want to drop or separate rail lines and the temporary blue
# line shuttles from the 2019 blue line construction
blue_line_shuttles = ["856", "860", "861", "862", "863", "864"]
rail_lines = {
    "801": "Blue",
    "802": "Red",
    "803": "Green",
    "804": "Gold",
    "805": "Purple",
    "806": "Expo",
}

# Convert year/month to a datetime and get a simplified time series
ridership_timeseries = weekday_ridership.assign(
    date=(
        pandas.to_datetime(
            weekday_ridership.year.astype(str) +
            "-" +
            weekday_ridership.month.astype(str)
        )
    )
)[["date", "line", "ridership"]]

# Drop the blue line shuttles
ridership_timeseries = ridership_timeseries[
    ~ridership_timeseries.line.isin(blue_line_shuttles)
]

# Separate raile lines and bus lines
rail_ridership_timeseries = ridership_timeseries[
    ridership_timeseries.line.isin(rail_lines.keys())
].assign(line_name=lambda x: x.line.map(rail_lines))
bus_ridership_timeseries = ridership_timeseries[
    ~ridership_timeseries.line.isin(rail_lines.keys())
]

# Finally, select data from 2020 onward, and add a column
# for 2019 ridership to compare.
bus_ridership_data = (
    bus_ridership_timeseries
    .loc[bus_ridership_timeseries.date > "2019"]
    .merge(
        average_ridership_2019.rename(columns={"ridership": "ridership_2019"}),
        how="inner",
        on="line",
    )
    .merge(
        average_ridership_2018.rename(columns={"ridership": "ridership_2018"}),
        how="inner",
        on="line",
    )
)

rail_ridership_data = (
    rail_ridership_timeseries
    .loc[rail_ridership_timeseries.date > "2019"]
    .merge(
        average_ridership_2019.rename(columns={"ridership": "ridership_2019"}),
        how="inner",
        on="line",
    )
    .merge(
        average_ridership_2018.rename(columns={"ridership": "ridership_2018"}),
        how="inner",
        on="line",
    )
)

# Checkpoint to CSV / parquet
rail_ridership_data.to_csv("metro-rail-covid-recovery.csv", index=False)
bus_ridership_data.to_csv("metro-bus-covid-recovery.csv", index=False)
rail_ridership_data.to_parquet("metro-rail-covid-recovery.parquet", index=False)
bus_ridership_data.to_parquet("metro-bus-covid-recovery.parquet", index=False)
