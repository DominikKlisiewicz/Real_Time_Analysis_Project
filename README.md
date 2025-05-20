# ✈️ Real-Time Flight Analytics with Kafka

This project was developed for the course **Real-Time Analytics** at SGH. It allowed our team to explore **Kafka**, real-time **stream data processing**.



---

## 🧑‍💻 Authors

- Dominik Klisiewicz ([GitHub @DominikKlisiewicz](https://github.com/DominikKlisiewicz))
- Julia Czapla
- Paweł Drąszcz
- Julia Czapla
- Eryk Skrętowski
- Weronika Walczak

---

## 📚 Project Overview

The system consists of four main components:

- **Data Simulation and Streaming**
- **Data Collection and Analysis**
- **Web UI**
- **REST API**

This project provided valuable learning opportunities in both technical and project management aspects. Dominik Klisiewicz led the team and coordinated the development efforts.

---

## 🛰️ Data Simulation and Streaming

As of June 2024, all real-time flight status APIs were paid, so the team opted to generate **synthetic flight data**:

- Initially, random x and y coordinates were generated and moved toward Warsaw.
- This naive approach failed due to the Earth's curvature.
- The problem was solved by using the **Haversine distance formula** to simulate more realistic flight paths.

The simulated events were streamed to Kafka using a `KafkaProducer`. Each event belonged to one of the following categories:

- `new_location`
- `flight_landed`
- `flight_crashed`

---

## 📊 Data Collection and Analysis

A script named `consumer_kafka.py` performed the following:

- Acted as a Kafka consumer subscribing to the flight data topic.
- Maintained the current state of each flight using a `pandas.DataFrame`.
- Calculated the **ETA** (estimated time of arrival) based on GPS coordinates alone.

---

## 🌐 Web UI

To visualize the system in action, a simple **Flask** web app was developed.

Users can:

- View historical flight data.
- Monitor live updates of incoming flights.

---

## 🔌 API

The application also exposes a **REST API**, enabling third-party apps to interact with the system programmatically.

---

## 🛠️ Technologies Used

- Python
- Flask
- Kafka (Producer & Consumer)
- Pandas
- HTML/CSS (for web interface)

---



