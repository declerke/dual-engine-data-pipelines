# 🚀 Dual-Engine Data Pipelines: Pandas vs. PySpark

This repository demonstrates a professional data engineering workflow comparing Pandas (local memory-optimized) and PySpark (distributed computing). The pipeline processes large-scale Retail (Superstore) and Real Estate (Airbnb) datasets, moving from raw ingestion to a structured Data Warehouse design.



---

## 🎯 Project Overview
This project benchmarks two industry-standard processing engines across identical ETL stages. It highlights the transition from single-machine data analysis to scalable, distributed big data engineering.

**Pipeline Stages:**
* **Data Ingestion:** Loading raw, messy datasets into staging environments.
* **Data Profiling:** Automated generation of statistical quality reports.
* **Data Cleaning:** Handling missing values, outliers, and schema enforcement.
* **Time Series Modeling:** Forecasting trends with ARIMA and Prophet.
* **Warehouse Design:** Converting flat files into an OLAP-ready Star Schema.



---

## 🏗️ Project Structure
```text
dual-engine-data-pipelines/
├── config/               
├── data/                 
├── notebooks/
│   ├── pandas/           
│   └── pyspark/          
├── reports/              
│   ├── figures/          
│   └── html/             
├── src/                  
├── tests/                
├── bin/                  
├── environment.yml       
└── requirements.txt      
```

---

## ⚡ Framework Comparison Matrix

| Feature | Pandas | PySpark |
| :--- | :--- | :--- |
| **Best For** | < 10GB Data | > 10GB Data |
| **Processing** | Single-machine / Eager | Distributed / Lazy |
| **Optimization** | Vectorization | Catalyst & Tungsten Engine |
| **Scalability** | Vertical (RAM bound) | Horizontal (Scalable Nodes) |

---

## 🔧 Installation & Setup

### Prerequisites
* Python 3.11 (Required for PySpark 3.5+)
* Java 17 (Specifically mapped to `C:\jdk17` for stability)
* Hadoop Home (`winutils.exe` in `C:\hadoop\bin`)

### Environment Setup
```powershell
conda env create -f environment.yml
conda activate spark-project
```

### Windows Spark Fix
To ensure stable communication between Python and the Java Gateway on Windows, the Spark Session is configured to bind to local loopback:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .getOrCreate()
```

---

## 📊 Analytics & Reporting
The pipeline produces rich artifacts stored in the `reports/` directory:
* **Figures:** Visualization of time-series forecasts and feature correlations.
* **HTML Reports:** Comprehensive data audits that identify drift, skewness, and cardinality issues before the data enters the warehouse.

---

## 🎓 Skills Demonstrated
* **Data Engineering:** ETL design, Dimensional Modeling (Kimball), Parquet optimization.
* **Big Data:** Spark Session management, Py4J Gateway troubleshooting.
* **DevOps:** Environment isolation, multi-framework configuration management.
* **Analytics:** Time-series decomposition and statistical profiling.
