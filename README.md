# Cyclistic Business Intelligence Analyst

## Project Overview
This repository contains documents and resources related to the **Cyclistic Business Intelligence Analyst** project. The project focuses on analyzing customer usage patterns for **Cyclistic**, a fictional bike-share company in **New York City**.

## Available Documents
- [**Cyclistic_Follow_Up_Question.pdf**](Documents/Cyclistic_follow_up_question.pdf): Contains follow-up questions and clarifications for the project.
- [**Cyclistic_Project_Requirements.pdf**](Documents/Cyclistic_Project_Requirements.pdf): Details the project scope, deliverables, and success criteria.
- [**Cyclistic_Stakeholder_Requirements.pdf**](Documents/Cyclistic_Stakeholder_Requirements.pdf): Outlines the key stakeholders and their expectations.
- [**Cyclistic_Strategy_Document.pdf**](Documents/Cyclistic_Strategy_Document.pdf): Provides strategic insights for customer growth and station expansion.
- [**NYC-citibike.pdf**](Documents/NYC-citibike.pdf): Summary report of trip volume, user behavior, and trip route patterns in New York.

## 📊 Cyclistic Business Intelligence Report Summary

This section summarizes the three key pages of the NYC Citibike dashboard report, with visuals and detailed breakdowns to guide business insights and analysis.

---

### 📄 Page 1: Start Location Analysis

![Start Station Location - Report Page 1](pic/report-1.png)

This page provides a detailed view of **trip activity by start station**, focusing on trip volume and duration across **Manhattan neighborhoods**. It distinguishes usage between **Subscribers** and **Customers**, highlighting differences in volume and trip time.

#### Key Features:
- **Map visualization**: Blue circles represent trip density per station.
- **Tabular summary**: Breakdown by borough and neighborhood with:
  - Number of trips
  - Average trip duration (Subscribers vs Customers)

#### Interactive Filters:
- **Date range**
- **Time of Day**
- **Season**
- **User Type**
- **Start Neighborhood**

This page helps identify high-volume pickup areas and typical trip lengths based on user segments.

---

### 📄 Page 2: Start–Destination Flow & Congestion

![Trip Flows & Top Routes - Report Page 2](pic/report-2.png)

This page analyzes trips as **start-to-destination pairs** at both **station** and **neighborhood** levels. It quantifies usage patterns and **trip congestion** via average speed.

#### Key Features:
- **Top Destination Neighborhoods**: Ranked by trip count (e.g., Chelsea-Hudson Yards, East Village).
- **Station-to-Station Flows**: Lists route pairs with:
  - Start & destination station
  - Neighborhood pairing
  - Number of trips
  - **Average speed (km/h)** — used as a congestion indicator

#### Interactive Filters:
- **Date Range**
- **Time of Day**
- **Season**
- **Start/Destination Neighborhood**
- **Rain Condition**

This section is ideal for identifying commuting patterns, high-demand travel corridors, and potentially congested routes.

---

### 📄 Page 3: Monthly Trends & Net Flow Analysis

![Monthly Trips and Net Flow - Report Page 3](pic/report-3.png)

This page focuses on **temporal patterns** and **station-level balance**, offering a macro and micro view of operational dynamics.

#### Part 1: Total Trips by Month
- **Line chart** shows monthly ridership from **Jan 2014 to Dec 2016**.
- Split by user type (Subscriber vs Customer).
- Reveals seasonal peaks and long-term growth trends.

#### Part 2: Net Flow Analysis
- Measures station-level flow:
  
  ```
  Net Flow = Trips Ended - Trips Started
  ```

- Indicates whether a station is a net **trip origin or destination**.
- Used to detect imbalances and manage fleet distribution.

##### 📊 Net Flow Interpretation Table

| **Net Flow Value** | **Meaning**                                                                                     | **Potential Location Analysis**                                             |
|--------------------|-------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------|
| `> 0`              | More trips **ended** than started → Indicates a **destination station**.<br>🟢 Potential **bike surplus**.             | Likely **residential areas**, **evening hotspots**, or **parks**.           |
| `< 0`              | More trips **started** than ended → Indicates an **origin station**.<br>🔴 Potential **bike shortage**.                | Common in **office zones**, **commuter hubs**, or **campuses**.             |
| `= 0`              | Balanced station usage → Equal inbound and outbound flow.<br>⚖️ **No imbalance**, efficient flow.                     | Typically found in **mixed-use areas**, like **downtowns** or **tourist areas**. |

#### Filters Available:
- **Date Range**
- **Time of Day**
- **Borough**
- **Neighborhood**

This page is especially valuable for **logistics teams**, **urban planners**, and **data-driven policy design**.


---

## Setup Instructions
This project is containerized using Docker and can be managed using `make`. Follow these steps to set up and run the environment.

### 1. **Build the Docker Environment**
```bash
make build          # Build the default Docker environment
make build-nc       # Build without cache
make build-progress # Build with detailed progress output
```

### 2. **Running the Project**
```bash
make run          # Start the Spark cluster
make run-d        # Start in detached mode
make run-scaled   # Start with scaled Spark workers (3 workers)
```

### 3. **Stopping the Environment**
```bash
make stop         # Stop the Spark cluster
make down         # Remove all containers and volumes
```

### 4. **Submitting Spark Jobs**
You can submit Spark jobs using:
```bash
make submit app=<script_name>.py
```

### 5. **Removing Results**
```bash
make rm-results  # Remove result files from `book_data/results/`
```

## Spark Applications

All Spark jobs are stored in the `spark_apps/` directory:

- [**bike_trips_parquet.py**](spark_apps/bike_trips_parquet.py)  
  **Purpose**: Loads and preprocesses bike trip data.  
  **Output**: Saves Parquet files to **GCS**, sets up an **external table** in **BigQuery**.

- [**get_weather.py**](spark_apps/get_weather.py)  
  **Purpose**: Collects and loads **weather data** into **BigQuery** for usage correlation analysis.

- [**station_location.py**](spark_apps/station_location.py)  
  **Purpose**: Aggregates **station metadata** and **location info**, uploads to **BigQuery** for spatial analysis.

Run any of them using:
```bash
make submit app=<script_name>.py
```

---

## Service Account Setup

To enable **Google Cloud BigQuery** integration, follow these steps:

1. **Place your Google Service Account key file in the `gcp/` directory:**
   ```bash
   mkdir -p gcp
   mv your-service-account.json gcp/
   ```

2. **Ensure the `docker-compose.yml` mounts the file correctly:**
   ```yaml
   volumes:
     - ./gcp:/opt/gcp
   environment:
     - GOOGLE_APPLICATION_CREDENTIALS=/opt/gcp/your-service-account.json
   ```

3. **Check if the file is accessible inside the container:**
   ```bash
   docker exec -it da-spark-master ls -l /opt/gcp
   ```

4. **Verify environment variables inside the container:**
   ```bash
   docker exec -it da-spark-master env | grep GOOGLE_APPLICATION_CREDENTIALS
   ```

   Expected output:
   ```
   GOOGLE_APPLICATION_CREDENTIALS=/opt/gcp/your-service-account.json
   ```

5. **Use the credentials in your Spark job:**
   ```python
   import os

   os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/opt/gcp/your-service-account.json"
   ```

Your Spark cluster is now ready to interact with **Google Cloud BigQuery** for scalable ETL workflows. 🚴‍♂️☁️📊
