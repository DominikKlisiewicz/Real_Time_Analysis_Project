print("Booting the consumer...")
from kafka import KafkaConsumer
import json
import time
import math
import numpy as np
import pandas as pd
import string
import math
from datetime import datetime as dt
from datetime import timedelta 
from math import sin, cos, sqrt, atan2, radians
import os

print("All libraries have been loaded")

EVENT_NEW_LOCATION = "new_location"
EVENT_EMERGENCY = "flight_emergency_landing"
EVENT_CRASH = "flight_crashed"
EVENT_PLANE_LANDED = "flight_landed"
EVENT_ALL_LANDED = "all_flights_landed"

FLIGHT_NUMBER = "flight_number"
IS_FLYING = "is_flying"
LANDED = "landed"

LAT = 'latitude'
LON = 'longtitude'
DISTANCE = "Distance"
TIME_DISTANCE = "TimeDistance"
ACTUAL_ARRIVAL = "ActualArrival"
SCHEDULED_ARRIVAL = "ScheduledArrival"
ALTITUDE = "Altitude"
VECTOR_LAT = "LatVector"
VECTOR_LON = "LonVector"
VECTOR_LAT_SEC = "LatVectorSec"
VECTOR_LON_SEC = "LonVectorSec"
EARLY = "Early"
LATE = "Late"
CURRENT_TIME = "DataGenerationTime"
FLIGHT_SPEED = "FlightSpeed"
AIRPORT_LAT = 52.17371
AIRPORT_LON = 20.96501
TIME = "time"


csv_path = os.path.join(os.getcwd(), "..", 'data', 'flight_data.csv')
flights_history_path = os.path.join(os.getcwd(), "..", 'data', 'output', 'flight_history_data.csv')
current_status_path = os.path.join(os.getcwd(), "..", 'data', 'output', 'current_flight_data.csv')


flights_df = pd.read_csv(csv_path)
flights_df = flights_df[["FlightNo"]]
flights_df.columns = ['flight_number']  
flights_df[["time", "longtitude", "latitude", "altitude", "event", "eta", IS_FLYING, LANDED]] = None


history_df = pd.DataFrame(columns=flights_df.columns)

def calculate_distance_from_airport(row):
    # Approximate radius of earth in km
    R = 6373.0

    lat1 = radians(AIRPORT_LAT)
    lon1 = radians(AIRPORT_LON)
    lat2 = radians(row[LAT])
    lon2 = radians(row[LON])

    dlon = abs(lon2 - lon1)
    dlat = abs(lat2 - lat1)

    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    distance = R * c
    
    # print("Result: ", distance)
    return distance

def haversine_distance(lat1, lon1, lat2, lon2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    # Convert decimal degrees to radians
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])

    # Haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    # Radius of earth in kilometers is 6371
    distance = 6372 * c
    
    return distance

print("Creating kafka consumer object...")
consumer = KafkaConsumer(
    'my_topic',  # Replace with your topic name
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='my-group',  # Replace with your group id
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

is_kafka_drinking_coffe = True

def consume_messages():
    for message in consumer:
        try:
            flight_data = message.value
            event=flight_data.get("event")
            print(f'Event: [{flight_data.get("event")}] ------------------------')
        except Exception as e:
            print(f"Exception occured for message:{message}, {e}")

        if event == EVENT_NEW_LOCATION:
            parse_new_location(flight_data)
        elif event == EVENT_PLANE_LANDED:
            parse_event_plane_landed(flight_data)
        elif event == EVENT_CRASH or event == EVENT_EMERGENCY:
            parse_event_plane_emergency(flight_data, event)
        else:
            continue
        #elif event == EVENT_ALL_LANDED:
        #    parse_event_all_landed(flight_data)
        
        flights_df.to_csv(current_status_path)
        history_df.to_csv(flights_history_path)
        print(is_kafka_drinking_coffe)
        
        if not is_kafka_drinking_coffe:
            break

    print("kafka ended up drinking coffe")

def parse_new_location(flight_data):
    current_flight_no = flight_data[FLIGHT_NUMBER]
    
    current_flight_index = flights_df[flights_df[FLIGHT_NUMBER] == current_flight_no].index

    print(current_flight_index)

    if current_flight_index.empty:
        print("empty")
        return

    current_flight_index = current_flight_index[0]

    # first appearance of the flight data
    if flights_df.at[current_flight_index, "event"] == None:
        data_to_save = flight_data.copy()
        data_to_save["landed"] = False
        data_to_save["is_flying"] = True
        data_to_save['eta'] = None
        flights_df.loc[current_flight_index] = data_to_save
        return


    # calculate
    current_row = flights_df.loc[current_flight_index]

    print(f"current_row: {current_row}")
    print(f"data to update: {flight_data}")

    lat1 = float(current_row[LAT])
    lon1 = float(current_row[LON])
    time1 = pd.to_datetime(current_row[TIME])

    lat2 = flight_data.get(LAT)
    lon2 = flight_data.get(LON)
    time2 = pd.to_datetime(flight_data[TIME])

    print(f"current time: {time1}, new time: {time2}")

    distance_in_time = haversine_distance(lat1, lon1, lat2, lon2)
    distance_to_airport = calculate_distance_from_airport(flight_data)
    print(f"distance: {distance_to_airport}")
    time_difference = (time2 - time1).total_seconds()
    print(f"time diff: {time_difference}")
    current_speed = distance_in_time / time_difference
    print(f"current speed: {current_speed*3600} km/h")
    time_to_arrival = pd.Timedelta(seconds=distance_to_airport/current_speed)
    print(f"time to arrival: {time_to_arrival}")
    estimated_time_of_arrival = time2 + time_to_arrival
    data_to_save = flight_data.copy()
    data_to_save['eta'] = estimated_time_of_arrival
    print(f"ETA: {estimated_time_of_arrival}")
    data_to_save["landed"] = False
    data_to_save["is_flying"] = True

    history_df.loc[len(history_df)] = current_row

    flights_df.loc[current_flight_index] = data_to_save



# def parse_event_plane_landed(flight_data):
#     data_to_save = flight_data.copy()
#     current_flight_no = flight_data[FLIGHT_NUMBER]
#     current_flight_index = flights_df[flights_df[FLIGHT_NUMBER] == current_flight_no].index
#     data_to_save[IS_FLYING] = False
#     data_to_save[LANDED] = True
#     data_to_save["event"] = EVENT_PLANE_LANDED
#     data_to_save["eta"] = None
#     data_to_save["longtitude"] = None
#     data_to_save["latitude"] = None
#     data_to_save["altitude"] = None
#     print(data_to_save.keys(), history_df.columns)
# #     flights_df.loc[flights_df[FLIGHT_NUMBER] == current_flight_no, [IS_FLYING, LANDED]] = [False, True]
#     current_flight_index = flights_df[flights_df[FLIGHT_NUMBER] == current_flight_no].index
#     current_row = flights_df.loc[current_flight_index]
#     history_df.loc[len(history_df)] = current_row
#     flights_df.loc[current_flight_index] = data_to_save

def parse_event_plane_landed(flight_data):
    data_to_save = {
        'flight_number': flight_data.get('flight_number'),
        'time': flight_data.get('time'),
        'event': EVENT_PLANE_LANDED,
        'is_flying': False,
        'landed': True,
        'eta': None,
        'longtitude': None,
        'latitude': None,
        'altitude': None
    }

    print(data_to_save.keys(), history_df.columns)
    current_flight_no = flight_data[FLIGHT_NUMBER]
    current_flight_index = flights_df[flights_df[FLIGHT_NUMBER] == current_flight_no].index

    if not current_flight_index.empty:
        flights_df.loc[current_flight_index, list(data_to_save.keys())] = list(data_to_save.values())
    else:
        flights_df.loc[len(flights_df)] = data_to_save
    current_row_series = pd.Series(data_to_save)
    history_df.loc[len(history_df)] = current_row_series

    print("Updated flights_df:")
    print(flights_df)
    print("Updated history_df:")
    print(history_df)

def parse_event_plane_emergency(flight_data, event):
    data_to_save = {
        'flight_number': flight_data.get('flight_number'),
        'time': flight_data.get('time'),
        'event': event,
        'is_flying': False,
        'landed': True,
        'eta': None,
        'longtitude': None,
        'latitude': None,
        'altitude': None
    }
    print(data_to_save.keys(), history_df.columns)
    current_flight_no = flight_data[FLIGHT_NUMBER]
    current_flight_index = flights_df[flights_df[FLIGHT_NUMBER] == current_flight_no].index

    if not current_flight_index.empty:
        flights_df.loc[current_flight_index, list(data_to_save.keys())] = list(data_to_save.values())
    else:
        flights_df.loc[len(flights_df)] = data_to_save
    current_row_series = pd.Series(data_to_save)
    history_df.loc[len(history_df)] = current_row_series

    print("Updated flights_df:")
    print(flights_df)
    print("Updated history_df:")
    print(history_df)
    
#     flights_df.loc[flights_df[FLIGHT_NUMBER] == current_flight_no, [IS_FLYING, LANDED]] = [False, False]
    current_flight_index = flights_df[flights_df[FLIGHT_NUMBER] == current_flight_no].index
    current_row = flights_df.loc[current_flight_index]
#     history_df.loc[len(history_df)] = current_row
    data_to_save = [event, None, flight_data.get('time'), False, False]
    flights_df.loc[current_flight_index, ["event","eta",'time', "is_flying", "landed"]] = data_to_save

def parse_event_all_landed(flight_data):
    global is_kafka_drinking_coffe 
    is_kafka_drinking_coffe = False


print("Starting to consume messages")
consume_messages()
