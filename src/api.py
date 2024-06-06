#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# Plik należu odpalić w terminalu komendą: python3 api.py
# z innego terminala można wysłać zapytanie do apicza
# Wzór zapytania dla historii lotu:
# curl -X GET "http://localhost:5000/api/v1.0/flight_data?flight_number=123&type=history"
# Wzór zapytania dla aktualnego położenia samolotu:
# curl -X GET "http://localhost:5000/api/v1.0/flight_data?flight_number=123&type=current"
# 123 należy zastąpić numerem lotu

import pandas as pd
from flask import Flask, request, jsonify

app = Flask(__name__)


current_flights_data_path = os.path.join(os.path.dirname(__file__), "../data/output/current_flight_data.csv")
current_flights_df = pd.read_csv(current_flights_data_path)


flight_history_data_path = os.path.join(os.path.dirname(__file__), "../data/output/flight_history_data.csv")
flight_history_df = pd.read_csv(flight_history_data_path)

@app.route('/api/v1.0/flight_data', methods=['GET'])
def get_flight_data():

    flight_number = request.args.get("flight_number")
    data_type = request.args.get("type")  # "current" lub "history"
    

    if flight_number is None:
        return jsonify({"error": "Flight number is required."}), 400
    if data_type not in ["current", "history"]:
        return jsonify({"error": "Type must be 'current' or 'history'."}), 400
    

    if data_type == "current":
        flight_data = current_flights_df[current_flights_df["flight_number"] == flight_number]
        if flight_data.empty:
            return jsonify({"error": "Flight not found."}), 404
        

        latitude = flight_data.iloc[0]["latitude"]
        longitude = flight_data.iloc[0]["longtitude"]
        time = flight_data.iloc[0]["time"]
        
        response = {
            "flight_number": flight_number,
            "time": time,
            "latitude": latitude,
            "longitude": longitude
        }
    
    elif data_type == "history":
        flight_history = flight_history_df[flight_history_df["flight_number"] == flight_number]
        if flight_history.empty:
            return jsonify({"error": "Flight not found."}), 404
        
 
        history = []
        for index, row in flight_history.iterrows():
            history.append({
                "time": row["time"],
                "latitude": row["latitude"],
                "longitude": row["longtitude"]
            })
        
        response = {
            "flight_number": flight_number,
            "history": history
        }
    
    return jsonify(response), 200

if __name__ == '__main__':
    app.run(debug=True)

