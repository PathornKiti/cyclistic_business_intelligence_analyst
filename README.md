# Cyclistic Business Intelligence Analyst

## Project Overview
This repository contains documents and resources related to the **Cyclistic Business Intelligence Analyst** project. The project focuses on analyzing customer usage patterns for **Cyclistic**, a fictional bike-share company in **New York City**.

## Available Documents
- [**Cyclistic_Follow_Up_Question.pdf**](Documents/Cyclistic_follow_up_question.pdf): Contains follow-up questions and clarifications for the project.
- [**Cyclistic_Project_Requirements.pdf**](Documents/Cyclistic_Project_Requirements.pdf): Details the project scope, deliverables, and success criteria.
- [**Cyclistic_Stakeholder_Requirements.pdf**](Documents/Cyclistic_Stakeholder_Requirements.pdf): Outlines the key stakeholders and their expectations.
- [**Cyclistic_Strategy_Document.pdf**](Documents/Cyclistic_Strategy_Document.pdf): Provides strategic insights for customer growth and station expansion.

## Setup Instructions
This project is containerized using Docker and can be managed using `make`. Follow these steps to set up and run the environment.

### **1. Build the Docker Environment**
To build the project, use the following commands:
```bash
make build          # Build the default Docker environment
make build-nc       # Build without cache
make build-progress # Build with detailed progress output
```

### **2. Running the Project**
To start the environment, run:
```bash
make run          # Start the Spark cluster
make run-d        # Start in detached mode
make run-scaled   # Start with scaled Spark workers (3 workers)
```

### **3. Stopping the Environment**
To stop the running containers:
```bash
make stop         # Stop the Spark cluster
```
To completely shut down and remove volumes:
```bash
make down         # Remove all containers and volumes
```

### **4. Submitting Spark Jobs**
You can submit Spark jobs using the following command:
```bash
make submit app=sample.py  # Submit a PySpark job
```

### **5. Removing Results**
To clear previous output results:
```bash
make rm-results  # Remove result files from `book_data/results/`
```

## **Service Account Setup**
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
   You should see `your-service-account.json` inside `/opt/gcp`.

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

Now, your Spark cluster is set up to authenticate with **Google Cloud BigQuery** for future ETL processes. 🚀
