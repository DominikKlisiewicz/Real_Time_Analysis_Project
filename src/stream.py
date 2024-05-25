from kafka import KafkaProducer
import json
import random
from datetime import datetime, timedelta
import time
import pandas as pd
import numpy as np
import math
from flights_data_generator import generate_flights_data
import os

# Data frame column names

flight_number = 'FlightNo'
lat = 'Latitude'
lon = 'Longitude'
distance = "Distance"
time_distance = "TimeDistance"
actual_arrival = "ActualArrival"
scheduled_arrival = "ScheduledArrival"
altitude = "Altitude"
vector_lat = "LatVector"
vector_lon = "LonVector"
vector_lat_sec = "LatVectorSec"
vector_lon_sec = "LonVectorSec"
early = "Early"
late = "Late"
current_altitude = "CurrentAltitude"
generation_time = "DataGenerationTime"
current_time = "CurrentTime"
flight_speed = "FlightSpeed"
base_speed = "BaseSpeed"

waw_lat = 52.17371
waw_lon = 20.96501

flight_speed_min = 840
flight_speed_max = 920

landing_distance = 160

random_speed_change_chance = 2

has_arrived = "HasArrived"
data_generation_time = "DataGenerationTime"
dt_format = '%Y-%m-%d %H:%M:%S.%f'


# generating data
shift_from_waw = 0.3

generate_flights_data(20, latitude_min=waw_lat-shift_from_waw, 
                      latitude_max=waw_lat+shift_from_waw,
                      longitude_min=waw_lon-shift_from_waw,
                      longitude_max=waw_lon+shift_from_waw)
start_time = datetime.now()
print("New flights data frame has been created.")
data_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'flight_data.csv')

flights_df = pd.read_csv(data_path)
flights_df[has_arrived] = False
flights_df[current_altitude] = flights_df[altitude]
flights_df[current_time] = pd.to_datetime(flights_df[generation_time])
flights_df[base_speed] = flights_df[flight_speed]

num_rows = flights_df.shape[0]


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
    distance = 6371 * c
    
    return distance

def calculate_distance_from_waw(row):
    """
    Calculate distance from WAW Chopin Airport
    using latitude and longtitude cooridnates
    """
    return haversine_distance(waw_lat, waw_lon, row[lat], row[lon])

def getNewPosition(current_position, destination, d_time, v_plane):
    """
    Used for determinig new plane coordinates after a given number of seconds
    """
    
    R = 6371  # Earth's radius in km
    d_distance = v_plane * d_time / 3600  # Convert velocity to km/s and multiply by time in seconds
    d_angle = d_distance / R
    current_lat, current_lon = np.radians(current_position)
    dest_lat, dest_lon = np.radians(destination)
    
    delta_lon = dest_lon - current_lon
    azimuth = np.arctan2(np.sin(delta_lon) * np.cos(dest_lat),
                         np.cos(current_lat) * np.sin(dest_lat) - 
                         np.sin(current_lat) * np.cos(dest_lat) * np.cos(delta_lon))
    
    new_lat = np.arcsin(np.sin(current_lat) * np.cos(d_angle) +
                        np.cos(current_lat) * np.sin(d_angle) * np.cos(azimuth))
    
    new_lon = current_lon + np.arctan2(np.sin(d_angle) * np.sin(azimuth) * np.cos(current_lat),
                                       np.cos(d_angle) - np.sin(current_lat) * np.sin(new_lat))
    
    return [np.degrees(new_lat), np.degrees(new_lon)]

# def checkSpeed(current_position, destination):
#     d = 2*R*np.arcsin(np.sqrt(np.sin(np.radians((destination[0]-current_position[0])/2))**2+np.cos(np.radians(current_position[0]))*np.cos(np.radians(destination[0]))*np.sin(np.radians((destination[1]-current_position[1])/2))**2))
#     return d*3600

def generate_new_coordinates(row):    
    v_plane = row[flight_speed]
    d_time = (datetime.now() - row[current_time]).total_seconds()
    new_lat, new_lon = getNewPosition([row[lat], row[lon]], [waw_lat, waw_lon], d_time, v_plane)
    new_time = datetime.now()
    return pd.Series([new_lat, new_lon, new_time])


def adjust_location():
    """
    This function is used to update flight location (latitude, longitude, altitude) as well as flight speed
    """
    num_rows = flights_df.shape[0]
    # Update location
    flights_df[[lat, lon, current_time]] = flights_df.apply(generate_new_coordinates, axis=1)
    
    # Update distance and time distance
    flights_df[distance] = flights_df.apply(calculate_distance_from_waw, axis=1)
    flights_df[time_distance] = flights_df[distance] / flights_df[flight_speed]
    
    # Determine landing status and progress
    landing_distance = 100  # Define a constant for when landing starts
    is_landing = flights_df[distance] <= landing_distance
    isnt_landing = ~is_landing
    
    # Calculate landing progress
    landing_progress = 1 - (flights_df[distance] / landing_distance)
    landing_progress = landing_progress.clip(lower=0)  # Ensure no negative values
    
    landing_progress_speed = 1 - (flights_df[distance] / landing_distance)
    landing_progress_speed = landing_progress.clip(lower=0)  # Ensure no negative values

    # Update current altitude for landing planes
    flights_df.loc[is_landing, current_altitude] = landing_progress[is_landing] * flights_df[altitude]
    flights_df.loc[isnt_landing, current_altitude] = flights_df[altitude]
    
    # Randomly change speed for planes not landing
    new_random_speed = np.random.uniform(flight_speed_min, flight_speed_max, size=num_rows)
    should_change = np.random.uniform(0, 100, size=num_rows) < random_speed_change_chance
    
    flights_df.loc[isnt_landing & should_change, flight_speed] = new_random_speed[isnt_landing & should_change]
    
    # Adjust speed for descending planes
    flights_df.loc[is_landing, flight_speed] = np.maximum((1 - landing_progress_speed.loc[is_landing]), 0.3) * flights_df[base_speed]
    
    print(flights_df[[current_altitude, flight_speed]])
    
    
def adjust_current_flights(producer, distance_from_airport = 15):
    """
    This function updates the data frame and deletes from it
    flights that are within a chosen radius from airport (distance_from_airport)
    """
    finished_flights = flights_df[distance] < distance_from_airport
    just_landed = flights_df[finished_flights][flight_number].tolist()
    flights_df.drop(flights_df[finished_flights].index, inplace=True)
    for flight in just_landed:
        now = datetime.now()
        message = {'flight_number': flight, 'time':str(now), 'event': 'flight_landed'}
        producer.send('my_topic', value=message)
        print(f"Flight {flight} has just landed")

        
def produce_messages():
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    
    try:
        while flights_df.shape[0] > 0:
            adjust_location()
            adjust_current_flights(producer)
            
            for index, row in flights_df.iterrows():
                
                should_skip = random.uniform(1, 10) < 1.5
                if should_skip:
#                     message = {
#                     'flight_number': "skipped" 
#                     }
#                     producer.send('my_topic', value=message)
                    continue
                
                id_row = row[flight_number]
                latitude_row = row[lat]
                longitude_row = row[lon]
                altitude_row = row[current_altitude]
                time_row = row[current_time]
                
                message = {
                    'flight_number': id_row,
                    'time': str(time_row),
                    'longtitude': longitude_row,
                    'latitude': latitude_row,
                    'altitude': altitude_row,
                    "event": "new_location"
                }
                producer.send('my_topic', value=message)
                print(f"Produced: {message}")

                sleep_time_ms = random.uniform(0.5, 1.5)
                time.sleep(sleep_time_ms)
        producer.send('my_topic', value={"event": "all_flights_landed"})
    except KeyboardInterrupt:
        producer.send('my_topic', value={"event": "streaming_interrupted"})
        producer.close()

if __name__ == "__main__":
    produce_messages()
