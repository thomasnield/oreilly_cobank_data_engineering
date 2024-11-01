# # Exercise
# We are going to merge two data "pipelines" straight from NOAA's website that gathers tornado data for both Texas and Oklahoma. We will then clean the data, select only the fields we are interested in, and load it into a SQLite database. 
# 
# STEP 1: First import the necessary libraries. 

from datetime import datetime
import pandas as pd
from IPython.core.display_functions import display
import sqlite3

# STEP 2: Declare the start date and end date as variables, which we can re-assign to whatever range we are interested in. 

start_date = datetime(2023, 1, 1)
end_date = datetime(2023, 12, 31)

# STEP 3: "Hack" the parameters in the two NOAA url's to use the `start_date` and `end_date`. Then use that create two pandas DataFrames for Texas and Oklahoma respectively. 

tx_url = f"https://www.ncdc.noaa.gov/stormevents/csv?eventType=%28C%29+Tornado&beginDate_mm={start_date.strftime('%m')}&beginDate_dd={start_date.strftime('%d')}&beginDate_yyyy={start_date.year}&endDate_mm={end_date.strftime('%m')}&endDate_dd={end_date.strftime('%d')}&endDate_yyyy={end_date.year}&county=ALL&hailfilter=0.00&tornfilter=0&windfilter=000&sort=DT&submitbutton=Search&statefips=48%2CTEXAS"
ok_url = f"https://www.ncdc.noaa.gov/stormevents/csv?eventType=%28C%29+Tornado&beginDate_mm={start_date.strftime('%m')}&beginDate_dd={start_date.strftime('%d')}&beginDate_yyyy={start_date.year}&endDate_mm={end_date.strftime('%m')}&endDate_dd={end_date.strftime('%d')}&endDate_yyyy={end_date.year}&county=ALL&hailfilter=0.00&tornfilter=0&windfilter=000&sort=DT&submitbutton=Search&statefips=40%2COKLAHOMA"

tx_df = pd.read_csv(?, dtype={"BEGIN_TIME" : str, "END_TIME" : str})
ok_df = pd.read_csv(?, dtype={"BEGIN_TIME" : str, "END_TIME" : str})

# STEP 4: Append the two DataFrames together. Display the resulting `DataFrame` which we will call `df`. Review the documentation to learn what each of these fields mean: https://www1.ncdc.noaa.gov/pub/data/swdi/stormevents/csvfiles/Storm-Data-Export-Format.pdf. 

df = ?

# STEP 5: Extract out only the fields of interest. 

fields = ["CZ_NAME_STR","BEGIN_LOCATION","BEGIN_DATE","BEGIN_TIME","TOR_F_SCALE",
          "DEATHS_DIRECT","INJURIES_DIRECT","DAMAGE_PROPERTY_NUM","DAMAGE_CROPS_NUM",
          "STATE_ABBR","END_LOCATION","END_DATE","END_TIME",
          "EVENT_NARRATIVE","EPISODE_NARRATIVE"]

df.drop(columns=[col for col in df if col not in ?], inplace=True)


# STEP 6: Convert date/time fields to a single datetime in new fields. Clean up the times so they have 4 digits and a colon. Then Convert those new fields to UTC. Finally, drop the original date/time fields.

def clean_time(time_str):
    c = f"{'0' * (4-len(time_str))}{time_str}"
    return c[0:2] + ":" + c[?:?]

df.insert(2, 'BEGIN_DATETIME', pd.to_datetime(df['BEGIN_DATE'] + ' ' + df['BEGIN_TIME'].apply(clean_time))  \
    .dt.tz_localize('US/Central') \
    .dt.tz_convert('UTC')
          )

df.insert(3, 'END_DATETIME', pd.to_datetime(df['END_DATE'] + ' ' + df['END_TIME'].apply(clean_time))  \
    .dt.tz_localize('US/Central') \
    .dt.tz_convert('UTC')
)

df.drop([?,?,?,?], axis=1, inplace=True)

# STEP 7: Rename a fiew fields to make them easier to identify for end users. 

df.rename(columns= { 
    ? : "COUNTY_NAME",
    ? : "DAMAGE_PROPERTY_USD", 
    ? : "DAMAGE_CROPS_USD"
})


# STEP 8: Load the data into a SQLite database file, into a table called `TORNADO_TRACK`. 

conn = sqlite3.connect('my_database.db')
df.to_sql("TORNADO_TRACK", conn, if_exists='replace', index=False)

# 4. VERIFY DATA IS LOADED USING A SELECT query 
sql_df = pd.read_sql(?, conn)
with pd.option_context('display.max_rows', None, 'display.max_colwidth', None):
  display(sql_df)

conn.close()