# SparkStructuredStreaming

This repository contains my learning exercises from the Udemy course [*Apache Spark and Databricks - Stream Processing in Lakehouse*](https://www.udemy.com/course/spark-streaming-using-python/learn/).

The course focuses on **Spark Structured Streaming** and its practical applications using the Databricks Community Edition environment.

## Environment & Tools

- **Platform:** Databricks Community Edition, Azure DataBricks
- **Notebook Type:** Jupyter notebooks  
- **File System:** DBFS (Databricks File System)  
- **Streaming Sources/Sinks:** File, Delta Table, Kafka (via Confluent Cloud in selected examples)

## Key Learnings

- Core concepts and **fundamentals of Spark Structured Streaming**
- **Architecture overview** and exploration of key **APIs**
- Understanding **triggers**, **checkpointing**, and **output modes**
- Executing streaming jobs in **batch mode** using the `AvailableNow` trigger
- Performing **stateful stream processing** with aggregations and state stores
- Building streaming pipelines using the **Medallion Architecture** (Bronze, Silver, Gold layers)
- Leveraging **RocksDB** as a state store and understanding its performance benefits
- Designing **idempotent streaming jobs** to ensure fault-tolerant processing
- Implementing aggregation strategies:
  - **Managed stateful aggregations**
  - **Custom stateless aggregations**
