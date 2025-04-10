import os
import sys
import time
import json
from unicodedata import numeric
from pytz import timezone
import requests
from datetime import datetime
from datetime import timedelta
import pandas as pd
from io import StringIO
from urllib.request import urlopen
import xmltodict
import numpy as np
import warnings
from sqlalchemy import create_engine
import inspect
import traceback

########################
# Utility functions    #
########################

# override print so each statement is timestamped
old_print = print
def timestamped_print(*args, **kwargs):
  old_print(datetime.now(), *args, **kwargs)
print = timestamped_print


def slicer(my_str,sub):
        index=my_str.find(sub)
        if index !=-1 :
            return my_str[index:] 
        else :
            raise Exception('Sub string not found!')
        
        
def postgres_upsert(table, conn, keys, data_iter):
    from sqlalchemy.dialects.postgresql import insert

    data = [dict(zip(keys, row)) for row in data_iter]

    insert_statement = insert(table.table).values(data)
    upsert_statement = insert_statement.on_conflict_do_update(
        constraint=f"{table.table.name}_pkey",
        set_={c.key: c for c in insert_statement.excluded},
    )
    conn.execute(upsert_statement)

#############################
# Method-specific functions #
#############################

def get_fiman_data(id, sensor, begin_date, end_date):
    """Retrieve data from specified sensor from the FIMAN API

    Args:
        id (str): Station id
        sensor (str): Requested sensor at station
        begin_date (str): Beginning date of requested time period. Format: %Y%m%d %H:%M
        end_date (str): End date of requested time period. Format: %Y%m%d %H:%M
        
    Returns:
        r_df (pd.DataFrame): DataFrame of requested data from specified station and time range. Dates in UTC
    """    
    print(inspect.stack()[0][3])    # print the name of the function we just entered
    
    #
    # It looks like if the data are not long enough (date-wise), the query to fiman will not return anything
    # at which point this will fail.
    #

    fiman_gauge_keys = pd.read_csv("data/fiman_gauge_key.csv").query("site_id == @id & Sensor == @sensor")
    
    new_begin_date = pd.to_datetime(begin_date, utc=True) - timedelta(seconds = 3600)
    new_end_date = pd.to_datetime(end_date, utc=True) + timedelta(seconds = 3600)
    
    query = {'site_id' : fiman_gauge_keys.iloc[0]["site_id"],
             'data_start' : new_begin_date.strftime('%Y-%m-%d %H:%M:%S'),
             'end_date' : new_end_date.strftime('%Y-%m-%d %H:%M:%S'),
             'format_datetime' : '%Y-%m-%d %H:%M:%S',
             'tz' : 'utc',
             'show_raw' : True,
             'show_quality' : True,
             'sensor_id' : fiman_gauge_keys.iloc[0]["sensor_id"]}
    print(query)    # FOR DEBUGGING

    # try:
    r = requests.get(os.environ.get("FIMAN_URL"), params=query, timeout=120)
    # except requests.exceptions.Timeout:
    #     return pd.DataFrame()

    j = r.content
    doc = xmltodict.parse(j)
    print(doc)
    
    unnested = doc["onerain"]["response"]["general"]["row"]
    
    r_df = pd.DataFrame.from_dict(unnested)

    r_df["date"] = pd.to_datetime(r_df["data_time"], utc=True); 
    r_df["id"] = str(id); 
    r_df["notes"] = "FIMAN"
    r_df["type"] = "water_level" if sensor == "Water Elevation" else "pressure"
    r_df = r_df.loc[:,["id","date","data_value","notes", "type"]].rename(columns = {"data_value":"value", "notes": "api_name"})

    return r_df.drop_duplicates(subset=['id', 'date'])

def get_noaa_data(id, type, begin_date, end_date):
    """Retrieve  data from the NOAA tides and currents API

    Args:
        id (str): Station id
        begin_date (str): Beginning date of requested time period. Format: %Y%m%d %H:%M
        end_date (str): End date of requested time period. Format: %Y%m%d %H:%M
        
    Returns:
        r_df (pd.DataFrame): DataFrame of requested data from specified station and time range. Dates in UTC
    """    
    print(inspect.stack()[0][3])    # print the name of the function we just entered

    query = {'station' : str(id),
             'begin_date' : begin_date.strftime("%Y%m%d %H:%M"),
             'end_date' : end_date.strftime("%Y%m%d %H:%M"),
             'product' : type,
             'units' : 'english',
             'datum': "NAVD",
             'time_zone' : 'gmt',
             'format' : 'json',
             'application' : 'Sunny_Day_Flooding_project, https://github.com/sunny-day-flooding-project'}
    print(query)

    r = requests.get('https://api.tidesandcurrents.noaa.gov/api/prod/datagetter/', params=query)
    
    j = r.json()

    if type == 'water_level':
        r_df = pd.DataFrame.from_dict(j["data"])
    else:
        r_df = pd.DataFrame.from_dict(j["predictions"])
    
    r_df['v'].replace('', np.nan, inplace=True)
    r_df["t"] = pd.to_datetime(r_df["t"], utc=True) 
    r_df["id"] = str(id) 
    r_df["type"] = type
    r_df["api_name"] = "NOAA"
    r_df = r_df.loc[:,["id","t","v","type","api_name"]].rename(columns = {"id":"id","t":"date","v":"value"})

    return r_df.dropna()

def get_hohonu_data(id, begin_date, end_date):
    """Retrieve data from specified sensor from the Hohonu API

    Args:
        id (str): Station id
        begin_date (str): Beginning date of requested time period. Format: %Y%m%d %H:%M
        end_date (str): End date of requested time period. Format: %Y%m%d %H:%M
        
    Returns:
        r_df (pd.DataFrame): DataFrame of requested data from specified station and time range. Dates in UTC
    """    

    print(inspect.stack()[0][3])    # print the name of the function we just entered

    new_begin_date = pd.to_datetime(begin_date, utc=True) - timedelta(seconds = 3600)
    new_end_date = pd.to_datetime(end_date, utc=True) + timedelta(seconds = 3600)

    query = {'datum' : 'NAVD',
             'from' : new_begin_date.strftime('%Y-%m-%d'),
             'to' : new_end_date.strftime('%Y-%m-%d'),
             'format' : 'json',
             'tz': '0',
             'cleaned': 'true'
    }
    print(query)    # FOR DEBUGGING

    url = "https://dashboard.hohonu.io/api/v1/stations/" + id + "/statistic"

    r = requests.get(url, params=query, timeout=120, headers={'Authorization': os.environ.get('HOHONU_API_TOKEN')})
    j = json.loads(r.content)
    r_df = pd.DataFrame({'timestamp': j['data'][0], 'value': j['data'][1]}).dropna()
    r_df["date"] = pd.to_datetime(r_df["timestamp"], utc=True); 
    r_df["id"] = str(id); 
    r_df["api_name"] = "Hohonu"
    r_df["type"] = "water_level"
    r_df = r_df.loc[:,["id","date","value","api_name", "type"]]

    return r_df.drop_duplicates(subset=['id', 'date'])

def main():
    print("Entering main of process_pressure.py")
    
    # from env_vars import set_env_vars
    # set_env_vars()
    
    ########################
    # Establish DB engine  #
    ########################

    SQLALCHEMY_DATABASE_URL = "postgresql://" + os.environ.get('POSTGRESQL_USER') + ":" + os.environ.get(
        'POSTGRESQL_PASSWORD') + "@" + os.environ.get('POSTGRESQL_HOSTNAME') + "/" + os.environ.get('POSTGRESQL_DATABASE')

    engine = create_engine(SQLALCHEMY_DATABASE_URL)
    
    #####################
    # Collect new data  #
    #####################

    now = pd.Timestamp("now", tz="UTC")

    # Get water level data

    # FIMAN
    stations = pd.read_sql_query("SELECT DISTINCT wl_id FROM sensor_surveys WHERE wl_src='FIMAN'", engine)
    stations = stations.to_numpy()

    for wl_id in stations:
        print("Querying FIMAN site " + wl_id[0] + "...")

        query = f"SELECT MAX(date) FROM api_data WHERE api_name='FIMAN' AND id='{wl_id[0]}' AND type='water_level'"
        
        start_date = pd.to_datetime(pd.read_sql_query(query, engine).iloc[0]['max'])
        date_limit = pd.to_datetime(now - pd.Timedelta(days=21))

        # Don't go further than 21 days back 
        if (start_date is None or start_date < date_limit):
            start_date = date_limit

        end_date = start_date + pd.Timedelta(hours=12)
        if (end_date > now):
            end_date = now
        
        new_data = get_fiman_data(wl_id[0], 'Water Elevation', start_date, end_date)

        if new_data.shape[0] == 0:
            warnings.warn("- No new raw data!")
            return
        
        print(new_data.shape[0] , "new records!")
        
        new_data.to_sql("api_data", engine, if_exists = "append", method=postgres_upsert, index=False)
        time.sleep(10)

    # Hohonu
    stations = pd.read_sql_query("SELECT DISTINCT wl_id FROM sensor_surveys WHERE wl_src='Hohonu'", engine)
    stations = stations.to_numpy()

    for wl_id in stations:
        print("Querying Hohonu site " + wl_id[0] + "...")

        query = f"SELECT MAX(date) FROM api_data WHERE api_name='Hohonu' AND id='{wl_id[0]}' AND type='water_level'"
        
        start_date = pd.to_datetime(pd.read_sql_query(query, engine).iloc[0]['max'])
        date_limit = pd.to_datetime(now - pd.Timedelta(days=21))

        # Don't go further than 21 days back 
        if (start_date is None or start_date < date_limit):
            start_date = date_limit

        end_date = start_date + pd.Timedelta(hours=24)
        if (end_date > now):
            end_date = now

        new_data = get_hohonu_data(wl_id[0], start_date, end_date)

        if new_data.shape[0] == 0:
            warnings.warn("- No new raw data!")
            return
        
        print(new_data.shape[0] , "new records!")
        
        new_data.to_sql("api_data", engine, if_exists = "append", method=postgres_upsert, index=False)
        time.sleep(10)

    # NOAA
    stations = pd.read_sql_query("SELECT DISTINCT wl_id FROM sensor_surveys WHERE wl_src='NOAA'", engine)
    alt_stations = pd.read_sql_query("SELECT DISTINCT alt_wl_id as wl_id FROM sensor_surveys WHERE alt_wl_src='NOAA'", engine)
    stations = pd.concat([stations, alt_stations]).drop_duplicates().reset_index(drop=True)
    stations = stations.to_numpy()

    for wl_id in stations:
        print("Querying NOAA site " + wl_id[0] + "...")

        # Observed data

        query = f"SELECT MAX(date) FROM api_data WHERE api_name='NOAA' AND id='{wl_id[0]}' AND type='water_level'"
        
        start_date = pd.to_datetime(pd.read_sql_query(query, engine).iloc[0]['max'])
        date_limit = pd.to_datetime(now - pd.Timedelta(days=21))

        # Don't go further than 21 days back 
        if (start_date is None or start_date < date_limit):
            start_date = date_limit

        end_date = start_date + pd.Timedelta(hours=24)
        # if (end_date > now):
        #     end_date = now

        new_data = get_noaa_data(wl_id[0], 'water_level', start_date, end_date)

        if new_data.shape[0] == 0:
            warnings.warn("- No new raw data!")
            return
        
        print(new_data.shape[0] , "new records!")
        
        new_data.to_sql("api_data", engine, if_exists = "append", method=postgres_upsert, index=False)
        time.sleep(10)

        # Predictions
        query = f"SELECT MAX(date) FROM api_data WHERE api_name='NOAA' AND id='{wl_id[0]}' AND type='predictions'"
        
        start_date = pd.to_datetime(pd.read_sql_query(query, engine).iloc[0]['max'])
        date_limit = pd.to_datetime(now - pd.Timedelta(days=21))

        # Don't go further than 21 days back 
        if (start_date is None or start_date < date_limit):
            start_date = date_limit

        if (start_date > now):
            start_date = now

        end_date = start_date + pd.Timedelta(days=2)
        # max_date = now + pd.Timedelta(days=2)
        # if (end_date > max_date):
        #     end_date = max_date

        new_data = get_noaa_data(wl_id[0], 'predictions', start_date, end_date)

        if new_data.shape[0] == 0:
            warnings.warn("- No new raw data!")
            return
        
        print(new_data.shape[0] , "new records!")
        
        new_data.to_sql("api_data", engine, if_exists = "append", method=postgres_upsert, index=False)
        time.sleep(10)

    # Get atm_pressure data

    # FIMAN
    stations = pd.read_sql_query("SELECT DISTINCT atm_station_id FROM sensor_surveys WHERE atm_data_src='FIMAN'", engine)
    stations = stations.to_numpy()

    for atm_station_id in stations:
        print("Querying FIMAN site " + atm_station_id[0] + "...")

        query = f"SELECT MAX(date) FROM api_data WHERE api_name='FIMAN' AND id='{atm_station_id[0]}' AND type='pressure'"
        
        start_date = pd.to_datetime(pd.read_sql_query(query, engine).iloc[0]['max'])
        date_limit = pd.to_datetime(now - pd.Timedelta(days=21))

        # Don't go further than 21 days back 
        if (start_date is None or start_date < date_limit):
            start_date = date_limit

        end_date = start_date + pd.Timedelta(hours=12)
        if (end_date > now):
            end_date = now
        
        new_data = get_fiman_data(atm_station_id[0], 'Barometric Pressure', start_date, end_date)

        if new_data.shape[0] == 0:
            warnings.warn("- No new raw data!")
            return
        
        print(new_data.shape[0] , "new records!")
        
        new_data.to_sql("api_data", engine, if_exists = "append", method=postgres_upsert, index=False)
        time.sleep(10)
    
    engine.dispose()

if __name__ == "__main__":
    main()
