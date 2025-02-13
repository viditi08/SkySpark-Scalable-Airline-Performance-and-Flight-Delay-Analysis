# Flight Delay Analysis with Kubernetes, Apache Spark, and Cassandra

## Project Overview ✈️
This project is designed to analyze **airline performance** and **flight delays** by leveraging a distributed computing environment powered by **Kubernetes**, **Apache Spark**, and **Cassandra**. The data is stored in **.parquet** format, ensuring optimized performance for large-scale analytics.

## System Architecture 🏗️

![architecture](https://github.com/user-attachments/assets/f353a700-a84b-4b71-ab2a-ae6ff549a0dc)

## Deployment Steps 🚀

### Step 1: Install Dependencies
Run the following script to install necessary dependencies:
```bash
sh requirements.sh
```

### Step 2: Launch Kubernetes Cluster
Initialize Minikube with adequate resources:
```bash
minikube start --cpus=4 --memory=6g
```

### Step 3: Verify Cluster Setup
Check running pods and cluster status:
```bash
kubectl get pods -A
```
Launch the Minikube dashboard for visual monitoring:
```bash
minikube dashboard
```

### Step 4: Deploy Cassandra Database
Navigate to the configuration directory and execute:
```bash
cd config
sh deploy-cassandra.sh
```

### Step 5: Install Spark Using Helm
Deploy Apache Spark into Kubernetes using Helm charts:
```bash
helm install spark-cluster bitnami/spark -f spark-config.yaml
```

### Step 6: Upload Data and Application Code
Copy the compiled application JAR file and `.parquet` data into the Spark master node:
```bash
kubectl cp path/to/jarfile.jar default/spark-master-0:/opt/spark
kubectl cp path/to/parquet-data default/spark-master-0:/opt/spark
```

### Step 7: Execute Spark Job
Run the Spark job for analyzing flight delays:
```bash
spark-submit \
--class com.analytics.FlightDelayProcessor \
--master k8s://https://kubernetes.default.svc \
--deploy-mode cluster \
--conf spark.executor.instances=3 \
--conf spark.executor.memory=4g \
--conf spark.executor.cores=2 \
FlightDelayAnalysis.jar "10.244.0.6" "9042"
```

### Step 8: Data Processing Steps
1. Establish connection with **Spark** and **Cassandra**.
2. Load and preprocess `.parquet` flight data.
3. Perform **data transformations** and **aggregations**.
4. Generate insights and store results in **Cassandra**.

### Step 9: Visualizing Results 📊
To analyze and visualize the processed flight delay data, execute:
```bash
python visualize_results.py
```
## Summary ✅
This project demonstrates how **Kubernetes**, **Spark**, and **Cassandra** can be orchestrated to analyze flight delays efficiently. By utilizing distributed computing and optimized data storage, the system provides valuable insights into airline performance.

💡 Have feedback or ideas? Open an issue or submit a pull request! 🚀

