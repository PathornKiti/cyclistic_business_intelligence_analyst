# Cyclistic Business Intelligence Analyst

## Project Overview
This repository contains documents and resources related to the **Cyclistic Business Intelligence Analyst** project. The project focuses on analyzing customer usage patterns for **Cyclistic**, a fictional bike-share company in **New York City**.

## Available Documents
- [**Cyclistic_Follow_Up_Question.pdf**](Documents/Cyclistic_follow_up_question.pdf): Contains follow-up questions and clarifications for the project.
- [**Cyclistic_Project_Requirements.pdf**](Documents/Cyclistic_Project_Requirements.pdf): Details the project scope, deliverables, and success criteria.
- [**Cyclistic_Stakeholder_Requirements.pdf**](Documents/Cyclistic_Stakeholder_Requirements.pdf): Outlines the key stakeholders and their expectations.
- [**Cyclistic_Strategy_Document.pdf**](Documents/Cyclistic_Strategy_Document.pdf): Provides strategic insights for customer growth and station expansion.
- [**NYC-citibike.pdf**](Documents/NYC-citibike.pdf): Summary report of trip volume, user behavior, and trip route patterns in New York.

## NYC Citibike Report Summary

### 📄 Page 1: Start Location Analysis

This page provides a detailed overview of **trip activity by start location**, focusing on both **trip volume** and **average trip duration** across various areas of New York City. The data is segmented by **borough** and **neighborhood**, enabling localized insights into ridership behavior.

#### 🔍 Key Metrics:
- **Number of Trips**: Total trips initiated from each start location.
- **Average Trip Duration** (in minutes): The average time taken per trip starting from that location.

#### 🗺️ Geographic Breakdown:
- **Borough**: High-level regional division (e.g., Manhattan, Queens).
- **Neighborhood**: Finer granularity within each borough (e.g., Chelsea-Hudson Yards, East Village).

#### 🎛️ Interactive Filters:
- **Season**: Compare ridership across different seasons (e.g., Spring vs. Winter).
- **Time of Day**: Analyze user behavior across morning, afternoon, evening, and night periods.

This visualization helps identify **popular pickup locations**, evaluate **temporal usage patterns**, and guide decisions related to **station placement**, **fleet distribution**, and **marketing strategies** tailored to specific locations and times.


### 📄 Page 2: Start–Destination Trip and Congestion Analysis

This page examines detailed **trip flows between stations**, providing a pairwise analysis of **start and destination locations** at both the **station** and **neighborhood** levels.

#### 🔁 Key Metrics:
- **Start Station → Destination Station**: Each route is listed with clear start and end points.
- **Neighborhood Pairing**: Each station is mapped to its respective neighborhood to understand broader area-level traffic.
- **Number of Trips**: Total volume for each start–destination pair.
- **Average Speed (km/h)**: A congestion proxy—lower speeds typically indicate higher traffic density or urban congestion.

#### ⚙️ Interactive Filters:
- **Season**: Analyze trip flows by time of year to identify seasonal commuting trends.
- **Time of Day**: Filter routes based on time segments (e.g., morning rush vs. evening leisure).
- **Rain Condition**: A weather filter that distinguishes trip behavior on **rainy days** versus **clear weather**, using precipitation data.

This analysis allows users to:
- Pinpoint **popular travel corridors** and **busiest connections**.
- Detect **high-congestion zones** using average speed as an indicator.
- Compare user behavior under **varying time and weather conditions**, essential for planning bike redistribution, improving route infrastructure, and forecasting demand under different scenarios.


### 📄 Page 3: Net Flow & Monthly Trip Trend Analysis

This page provides two key insights into system performance:

1. **Total Trips by Month**: Visualized as a time series to track long-term ridership patterns.
2. **Station-Level Net Flow**: Highlights imbalances between bike pickups and drop-offs.

---

#### 📈 Monthly Trip Trends
- A **line chart** displays total monthly trips across the selected date range.
- Segmented by user type (e.g., **Subscriber** vs. **Customer**).
- Useful for identifying **seasonal fluctuations**, usage spikes, and **growth trends**.

---

#### 🔄 Net Flow Analysis

The **net flow** metric captures the difference between the number of bikes returned and the number checked out at each station:

```
Net Flow = Trips Ended - Trips Started
```

This reflects the **station's directional bias**—whether it's primarily a **trip origin** or **destination**.

---

#### 📊 Net Flow Interpretation Table

| **Net Flow Value** | **Meaning**                                                                                                           | **Potential Location Analysis**                                             |
|--------------------|-----------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------|
| `> 0`              | More trips **ended** than started → Indicates a **destination station**.<br>🟢 Potential **bike surplus**.             | Likely **residential areas**, **evening hotspots**, or **parks**.           |
| `< 0`              | More trips **started** than ended → Indicates an **origin station**.<br>🔴 Potential **bike shortage**.                | Common in **office zones**, **commuter hubs**, or **campuses**.             |
| `= 0`              | Balanced station usage → Equal inbound and outbound flow.<br>⚖️ **No imbalance**, efficient flow.                     | Typically found in **mixed-use areas**, like **downtowns** or **tourist areas**. |


---

#### 🎛️ Available Filters
- **Date Range**: Define custom time intervals for analysis.
- **Time of Day**: Segment flow patterns by morning, afternoon, evening, and night.
- **Borough / Neighborhood**: Analyze net flow in specific regions or local zones.

---

#### 🧠 Additional Insight

- **Positive Net Flow** zones suggest where users often **end their journeys**—likely **home neighborhoods** or **social venues**.
- **Negative Net Flow** zones suggest **trip origin points**—often **business districts**, **train stations**, or **schools**.

This insight helps:
- **Urban planners** design more efficient systems,
- **Fleet managers** optimize bike rebalancing,
- And **stakeholders** make informed, location-aware decisions.


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
