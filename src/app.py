from flask import Flask, jsonify, render_template
import pandas as pd
import os

app = Flask(__name__)

# Define the paths to the CSV files
data_path = os.path.join(os.path.dirname(__file__), '..', 'data')
current_status_path = os.path.join(data_path, 'output', 'current_flight_data.csv')
flights_history_path = os.path.join(data_path, 'output', 'flight_history_data.csv')

@app.route('/')
def index():
    return render_template('index.html')

def style_status(status):
    if status == "Opóźniony":
        return f'<span style="color: red;">{status}</span>'
    elif status == "Wylądował":
        return f'<span style="color: green;">{status}</span>'
    return status

@app.route('/current')
def current_flights():
    current_df = pd.read_csv(current_status_path)
    current_df['scheduled_arrival'] = pd.to_datetime(current_df['scheduled_arrival'])
    current_df['eta'] = pd.to_datetime(current_df['eta'],errors='coerce')
    current_df['TERMINAL'] = current_df['flight_number'].apply(lambda x: "Wyjście 1" if int(x[-1]) % 2 != 0 else "Wyjście 2")
    current_df['STATUS'] = current_df.apply(lambda row: "Wylądował" if row['landed'] == True else ("Opóźniony" if row['eta'] > row['scheduled_arrival'] else ""), axis=1)
    current_df = current_df[['flight_number', 'time', 'scheduled_arrival', 'TERMINAL', 'STATUS','eta']]
    current_df.columns = ['Numer lotu', 'Czas', 'Planowany czas przylotu', 'Terminal', 'Status','Szacowany czas przylotu']
    current_df['Status'] = current_df['Status'].apply(style_status)
    current_df['Szacowany czas przylotu'] = pd.to_datetime(current_df['Szacowany czas przylotu'],errors='coerce').dt.strftime('%H:%M')
    current_df['Planowany czas przylotu'] = pd.to_datetime(current_df['Planowany czas przylotu']).dt.strftime('%H:%M')
    current_df['Szacowany czas przylotu'] = current_df['Szacowany czas przylotu'].apply(lambda x: "" if pd.isnull(x) else x)
    html_table = current_df.to_html(classes='table', escape=False, index=False)
    return jsonify({'html_table': html_table})

@app.route('/historical')
def historical_flights():
    historical_df = pd.read_csv(flights_history_path)
    historical_df['scheduled_arrival'] = pd.to_datetime(historical_df['scheduled_arrival'])
    historical_df['eta'] = pd.to_datetime(historical_df['eta'],errors='coerce')
    historical_df['TERMINAL'] = historical_df['flight_number'].apply(lambda x: "Wyjście 1" if int(x[-1]) % 2 != 0 else "Wyjście 2")
    historical_df['STATUS'] = historical_df.apply(lambda row: "Wylądował" if row['landed'] == True else ("Opóźniony" if row['eta'] > row['scheduled_arrival'] else " "), axis=1)
    historical_df = historical_df[['flight_number', 'time', 'scheduled_arrival', 'TERMINAL', 'STATUS','eta']]
    historical_df.columns = ['Numer lotu', 'Czas', 'Planowany czas przylotu', 'Terminal', 'Status','Szacowany czas przylotu']
    historical_df['Status'] = historical_df['Status'].apply(style_status)
    historical_df['Szacowany czas przylotu'] = pd.to_datetime(historical_df['Szacowany czas przylotu'],errors='coerce').dt.strftime('%H:%M')
    historical_df['Planowany czas przylotu'] = pd.to_datetime(historical_df['Planowany czas przylotu']).dt.strftime('%H:%M')
    historical_df['Szacowany czas przylotu'] = historical_df['Szacowany czas przylotu'].apply(lambda x: "" if pd.isnull(x) else x)
    html_table = historical_df.to_html(classes='table', escape=False, index=False)
    return jsonify({'html_table': html_table})

if __name__ == '__main__':
    app.run(debug=True)
