import numpy as np
import pandas as pd
import random
import string
import math
from datetime import datetime as dt
from datetime import timedelta 
from math import sin, cos, sqrt, atan2, radians
import os

# Data

# Europe Coordinates Limits
latitude_min_europe = 32.0
longitude_min_europe = -30.0

latitude_max_europe = 70.0
longitude_max_europ = 31.0

# WAW Airport coordinates
airport_lat = 52.17371
airport_lon = 20.96501

flight_speed_min = 840
flight_speed_max = 920


normal_altitude_min = 30000
normal_altitude_max = 37000


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
current_time = "DataGenerationTime"
flight_speed = "FlightSpeed"

start_time = dt.now()
np.random.seed(42)


# file destination path
save_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'flight_data.csv')
# save_path = "./data/flight_data.csv"

def generate_flight_number():
    airline_code = ''.join(random.choices(string.ascii_uppercase, k=np.random.randint(2, 3)))
    flight_number = ''.join(map(str, np.random.randint(0, 10, size=np.random.randint(3, 5))))    
    return airline_code + flight_number

def generate_flight_numbers(n):
    return [generate_flight_number() for _ in range(n)]

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

def calculate_distance_from_airport(row):
    # Approximate radius of earth in km
    R = 6373.0

    lat1 = radians(airport_lat)
    lon1 = radians(airport_lon)
    lat2 = radians(row[lat])
    lon2 = radians(row[lon])

    dlon = abs(lon2 - lon1)
    dlat = abs(lat2 - lat1)

    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    distance = R * c
    
    # print("Result: ", distance)
    return distance

def generate_altitude(remaining_time):
    if remaining_time > 0.5:
        altitude = np.random.uniform(normal_altitude_min, normal_altitude_max)
    else:
        descent_level = remaining_time / 0.5
        altitude = np.random.uniform(normal_altitude_min, normal_altitude_max) * descent_level
    return altitude
        
def calculate_flight_altitude(row):
    return generate_altitude(row[time_distance])

def generate_flights_data(num_of_incoming_flights = 30, airport_lat = 52.17371, airport_lon = 20.96501,
                          latitude_min = latitude_min_europe, latitude_max=latitude_max_europe, longitude_min=longitude_min_europe, longitude_max=longitude_max_europ,
                          sigma_lat=9, sigma_lon=6):
    """
    This function generates a pandas DataFrame with random realistic flight data.

    The user may choose: 
    num_of_incoming_flights - a desired number of flights to be generated 
    airport_lat - latitude of the destination airport
    airport_lon - longitude of the destination airport
    latitude_min, latitude_max, longitude_min, longitude_max - Area of the globe for the flights to be generated in
    """
    random_flight_numbers = generate_flight_numbers(num_of_incoming_flights)
    while(len(set(random_flight_numbers))!=num_of_incoming_flights):
        random_flight_numbers = generate_flight_numbers(num_of_incoming_flights)

    # mean_lat = (latitude_min + latitude_max)/2 
    # sigma_lat = 9  # Standard deviation of latitudes
    # random_lat = np.random.normal(mean_lat, sigma_lat, num_of_incoming_flights)

    # mean_lon = (longitude_min + longitude_max)/2  
    # sigma_lon = 6  # Standard deviation of longitudes
    # random_lon = np.random.normal(mean_lon, sigma_lon, num_of_incoming_flights)

    random_lat = np.random.uniform(latitude_min, latitude_max, size=num_of_incoming_flights)
    random_lon = np.random.uniform(longitude_min, longitude_max, size=num_of_incoming_flights)


    df = pd.DataFrame({flight_number: random_flight_numbers, lat: random_lat, lon: random_lon})
    df[distance] = df.apply(calculate_distance_from_airport, axis=1)
    df[flight_speed] = np.random.uniform(flight_speed_min, flight_speed_max, size=num_of_incoming_flights)
    df[time_distance] = df[distance]/df[flight_speed]
    df[vector_lat] = airport_lat - df[lat] 
    df[vector_lon] = airport_lon - df[lon]
    df[vector_lat_sec] = df[vector_lat] / (df[time_distance] * 3600)
    df[vector_lon_sec] = df[vector_lon] / (df[time_distance] * 3600)
    df[altitude] = df.apply(calculate_flight_altitude, axis=1)
    df[actual_arrival] = [start_time + timedelta(minutes=int(td * 60)) for td in df[time_distance]]

    # Generating Chi-squared distributed binary indicator of a skewed arrival time with more probability of 0
    chi2_indicators = np.random.chisquare(df[time_distance].mean(), size=len(df)) > df[time_distance].mean()
    binary_indicators = chi2_indicators.astype(int)

    # Generating delays/early arrivals from a normal distribution
    mu_delay = 18      # Mean delay
    sigma_delay = 15  # Standard deviation of delay
    delays = np.random.normal(mu_delay, sigma_delay, len(df))

    adjusted_delays = binary_indicators * delays

    # Add the delays to the scheduled arrival times to get the actual approximated arrival times
    df[scheduled_arrival] = df[actual_arrival] - pd.to_timedelta(adjusted_delays, unit='m')
    df[late] = df[actual_arrival] >df[scheduled_arrival]
    df[early] = df[actual_arrival] <df[scheduled_arrival]
    df[current_time] = start_time
    df.to_csv(save_path, index=False)
