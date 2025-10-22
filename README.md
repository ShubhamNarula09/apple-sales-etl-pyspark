# Apple Sales ETL Pipeline (PySpark Project)

## Project Overview
This project implements a **modular ETL pipeline** for Apple sales and customer datasets using **PySpark**.  

**Key features:**
- Reading data from multiple sources (CSV/Parquet/Delta)  
- Transforming data using Spark SQL and DataFrames  
- Optimized ETL with broadcast joins, partitioning, and bucketing  
- Storing processed outputs efficiently for analytics  

---

## Technologies & Tools
- **Python 3.6+**  
- **Apache Spark 3.2**  
- **PySpark SQL / DataFrames**  
- **Loguru** for logging  
- **ConfigParser** for configuration  
- **Git & GitHub** for version control  
- **unittest** for unit testing  

---

## Project Structure

<code> pyspark-project/
├─ README.md
├─ src/
│ ├─ loader_factory.py
│ ├─ reader_factory.py
│ ├─ transformer.py
│ ├─ workflows/
│ │ ├─ iphone_airpods_workflow.py
│ │ ├─ iphone_macbook_workflow.py
│ │ └─ high_value_customers_workflow.py
│ ├─ spark_manager.py
│ └─ main.py
├─ config/
│ └─ config.ini
├─ data/
│ ├─ AppleData/
│ │ ├─ customers.csv
│ │ ├─ products.csv
│ │ └─ transactions.csv
│ └─ outputfiles/
└─ tests/
└─ test_high_value_customers.py 
</code>

---

## Configuration

```ini
[AppleAnalysisCSV]
customers_df_path = data/AppleData/customers.csv
products_df_path = data/AppleData/products.csv
transactions_df_path = data/AppleData/transactions.csv

[OutputPath]
high_value_customers = data/outputfiles/high_value_customers

[sparkConfigurations]
appName = AppleAnalysis
master = local[4]
```

---
## Workflows

- **iPhone before AirPods**  
  *Identify customers who bought an iPhone immediately before AirPods.*

- **iPhone and MacBook sequence**  
  *Identify customers who bought an iPhone followed by a MacBook.*

- **High-value customers**  
  *Calculate total spending per customer and store outputs partitioned by location.*

> Each workflow inherits from an abstract `Workflow` class. Transformers are reusable across workflows.

---
## ETL Pipeline Design

- **Extractor**: *Reads raw data using `ReaderFactory`.*
- **Transformer**: *Performs transformations for each question.*
- **Loader**: *Writes DataFrames to disk with optional partitioning or bucketing.*
- **Workflow Runner**: *Orchestrates Extract → Transform → Load.*

**Example usage:**

```python
dataframes = AirpodsAfterIphoneExtractor().extract()
transformed_df = FirstTransform().transform(dataframes)
Loader(file_path=output_path).write(
    transformed_df,
    file_format="parquet",
    partition_by=["location"]
)
```

---
## Optimizations Implemented

- **Broadcast joins** for small reference tables (`customers`, `products`)  
- **Partitioning** on logical columns for efficient reads/writes  
- **Bucketing** on high-cardinality columns where applicable  
- **Generic FileDataSource** for CSV/Parquet/Delta formats  
- **Logging** with Loguru  

---

## Unit Testing

- Unit tests use **mock Spark DataFrames**  
- PySpark assertions verify **row counts, column existence, and sample values**  

**Run tests:**

```bash
python -m unittest discover -s tests
```
---

## 🚀 Setup & Run Instructions

**Clone repository:**

```bash
git clone https://github.com/<username>/apple-sales-etl-pyspark.git
cd apple-sales-etl-pyspark
```

**Create virtual environment & install dependencies:**
```
python -m venv .venv

# Windows
.venv\Scripts\activate

# Linux/Mac
source .venv/bin/activate

pip install -r requirements.txt
```

**Run a workflow:**
```
python src/main.py
```

**Output Files**
- Processed files will be in data/outputfiles/.

---
