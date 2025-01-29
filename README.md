# **Real-Time Air Quality and Weather Monitoring Using Apache Storm ⛅🚀**

This project implements a **real-time streaming data processing system** using **Apache Storm** to analyze **air quality levels** and **cloud coverage** across the U.S. The system ingests live weather data, applies **approximate frequency counting** using the **Lossy Counting Algorithm**, and generates top-k state reports for each category in real time.

---

## **Features 🛠️**
- **Real-Time Data Processing**: Uses Apache Storm to ingest and analyze live weather data.
- **Approximate Frequency Counting**: Implements the Lossy Counting algorithm for real-time top-k state estimation.
- **Scalable and Parallel Architecture**: Implements both **non-parallel** and **parallel** topologies with multiple computing nodes.
- **Cloud-Based Forecasting**: Integrates 3-day weather forecasts for cloud coverage analysis.
- **Optimized Performance**: Uses distributed processing and load balancing for efficient computation.

---

## **System Architecture 🏗️**
The system consists of two **Storm topologies**:
1. **Non-Parallel Topology** – Computes top 5 states per **EPA Air Quality Level**.
2. **Parallel Topology** – Computes top 5 states per **Cloud Coverage Level** over a 3-day forecast.

### **1. Data Sources 🌍**
- **WeatherAPI.com** – Provides real-time and forecasted weather data (API integration required).
- **US Zip Code Dataset** – Maps zip codes to states.

### **2. Apache Storm Components ⚡**
- **Spouts**:
  - **WeatherSpout**: Fetches live weather data for all zip codes.
  - **ForecastSpout**: Fetches 3-day cloud coverage forecasts.
  
- **Bolts**:
  - **LossyCountingBolt**: Implements the Lossy Counting algorithm for approximate top-k estimation.
  - **AggregationBolt**: Aggregates results and prepares reports.
  - **StorageBolt**: Saves reports to files.

---

## **Data Processing Workflow 🔄**
1. **Spouts** fetch real-time weather and forecast data.
2. **Bolts** process the stream using the **Lossy Counting Algorithm**.
3. **AggregationBolt** computes **top-k states** per category.
4. **StorageBolt** logs results in the following format:

```plaintext
<timestamp> <air_quality_level> <state1> <count1> <state2> <count2> ... <state5> <count5>
<timestamp> <cloud_coverage_level> <state1> <count1> <state2> <count2> ... <state5> <count5>
```

---

## **Example Output 📊**
**Air Quality Report (Every Minute)**
```plaintext
2025-01-30T14:00:00Z AQI-Level-1 California 120 Texas 110 Florida 95 New York 80 Illinois 70
2025-01-30T14:01:00Z AQI-Level-2 Texas 140 California 130 Georgia 100 Florida 90 Illinois 85
```

**Cloud Coverage Report (Every Minute)**
```plaintext
2025-01-30T14:00:00Z Cloud-Level-1 California 85 Arizona 75 Texas 65 Nevada 60 New Mexico 55
```

---

## **Highlights 🌟**
- **Stream Processing with Apache Storm** for real-time analytics.
- **Distributed and Scalable Architecture** using multi-node Storm clusters.
- **Efficient Approximate Frequency Counting** for large-scale data streams.
- **Parallel Processing for Enhanced Performance** in cloud coverage analysis.

---

## **Requirements 📋**
- **Java 8+**
- **Apache Storm 2.x**
- **Zookeeper**
- **Maven**
- **WeatherAPI Access Key**

---

## **Extra Features ⭐**
- **Bias Reduction in Zip Code Distribution**: Implements normalization to prevent states with more zip codes from dominating results.
- **Dynamic Rebalancing**: Uses Storm’s built-in resource management for optimal load balancing.
