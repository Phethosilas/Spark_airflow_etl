# Flexible Spark Airflow ETL Pipeline

This project provides a configurable ETL pipeline using Apache Spark and Apache Airflow that can handle any data size - from thousands to billions of records.

## 🚀 Key Features

- **Flexible Data Size**: Configure for small (1K), medium (1M), large (100M), or extra-large (1B+) datasets
- **Shared Spark Session**: Efficient resource usage across all tasks
- **Modular Design**: Each task in its own Python file for easy reading and maintenance
- **Astro CLI Compatible**: Built for Astronomer's Astro CLI environment
- **Auto-Scaling**: Spark configurations automatically adjust based on data size

## 📁 Project Structure

```
Spark_airflow_etl/
├── dags/
│   ├── spark_etl_dag.py          # Main configurable DAG
│   └── exampledag.py             # Original Astro example
├── include/
│   ├── spark_utils.py            # Spark session management
│   ├── database_preparation.py   # Database table creation
│   ├── database_connection.py    # Database connection utilities
│   ├── data_generation.py        # Spark data generation and loading
│   ├── database_finalization.py  # Database indexing and statistics
│   ├── spark_cleanup.py          # Spark session cleanup
│   ├── performance_estimation.py # Performance estimation
│   └── sample_data.csv           # Sample data file
├── tests/
│   └── dags/
│       └── test_dag_example.py   # DAG tests
├── Dockerfile                    # Astro runtime with Spark
├── requirements.txt              # Python dependencies
└── README.md                     # This file
```

## 🛠️ Setup Instructions

### Prerequisites
- Astro CLI installed
- Docker Desktop running
- PostgreSQL connection configured

### Quick Start

1. **Clone and start Astro:**
   ```bash
   git clone https://github.com/Phethosilas/Spark_airflow_etl.git
   cd Spark_airflow_etl
   astro dev start
   ```

2. **Access Airflow:**
   - Open http://localhost:8080
   - Login with admin/admin

3. **Configure PostgreSQL connection:**
   - Go to Admin > Connections
   - Create connection with ID: `postgres_default`
   - Set your PostgreSQL details

4. **Run the pipeline:**
   - Find `spark_etl_pipeline` DAG
   - Configure parameters (optional)
   - Trigger the DAG

## ⚙️ Configuration Options

### DAG Parameters

You can customize the pipeline by setting these parameters when triggering the DAG:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `num_records` | 1,000,000 | Number of records to generate |
| `table_name` | weather_data | Target PostgreSQL table name |
| `data_size` | medium | Spark optimization level |

### Data Size Categories

| Category | Records Range | Executor Memory | Use Case |
|----------|---------------|-----------------|----------|
| **small** | < 1M | 1g | Testing, development |
| **medium** | 1M - 100M | 2g | Regular ETL jobs |
| **large** | 100M - 1B | 4g | Big data processing |
| **xlarge** | 1B+ | 8g | Your original billion records |

## 🎯 Usage Examples

### Example 1: Small Test Run
```json
{
  "num_records": 10000,
  "table_name": "test_weather",
  "data_size": "small"
}
```

### Example 2: Medium Production Load
```json
{
  "num_records": 10000000,
  "table_name": "weather_data",
  "data_size": "medium"
}
```

### Example 3: Your Original Billion Records
```json
{
  "num_records": 1000000000,
  "table_name": "weather_billion",
  "data_size": "xlarge"
}
```

## 📊 Performance Expectations

Based on typical Spark performance:

| Data Size | Records | Est. Time | Rate |
|-----------|---------|-----------|------|
| Small | 10K | 1 second | 10K/sec |
| Medium | 1M | 20 seconds | 50K/sec |
| Large | 100M | 8 minutes | 200K/sec |
| XLarge | 1B | 33 minutes | 500K/sec |

*Note: Actual performance depends on your hardware and PostgreSQL setup*

## 🏗️ Architecture

### Workflow
1. **Performance Estimation** → Analyze parameters and estimate completion time
2. **Database Preparation** → Create optimized table structure
3. **Data Generation & Loading** → Generate records using Spark and load to PostgreSQL
4. **Database Finalization** → Add indexes and collect statistics
5. **Cleanup & Reporting** → Clean up resources and generate final report

### Modular Components

Each task is in its own file for easy reading and maintenance:

- **`spark_utils.py`**: Manages Spark session with size-based configurations
- **`database_preparation.py`**: Handles PostgreSQL table creation
- **`database_connection.py`**: Manages database connection properties
- **`data_generation.py`**: Generates and loads data using Spark
- **`database_finalization.py`**: Adds indexes and collects statistics
- **`spark_cleanup.py`**: Cleans up Spark resources
- **`performance_estimation.py`**: Provides performance estimates
- **`spark_etl_dag.py`**: Main DAG that orchestrates all tasks

## 🔧 Customization

### Adding New Data Sizes

Edit `include/spark_utils.py` to add new configurations:

```python
configs = {
    "tiny": {
        "spark.executor.memory": "512m",
        "spark.executor.cores": "1",
        # ... other settings
    },
    # ... existing configs
}
```

### Modifying Table Schema

Edit the table creation in `include/database_preparation.py`:

```python
cur.execute(f"""
    CREATE TABLE {table_name} (
        id BIGSERIAL PRIMARY KEY,
        # Add your custom columns here
        created_at TIMESTAMPTZ DEFAULT NOW()
    );
""")
```

### Adding New Tasks

1. Create a new file in `include/` directory
2. Define your function
3. Import and use it in `dags/spark_etl_dag.py`

## 🐛 Troubleshooting

### Common Issues

1. **Spark Out of Memory**
   - Increase `data_size` parameter (small → medium → large → xlarge)
   - Reduce `num_records` for testing

2. **PostgreSQL Connection Failed**
   - Verify connection details in Airflow UI
   - Ensure PostgreSQL is running and accessible

3. **Slow Performance**
   - Check Docker resource allocation
   - Monitor Spark UI at http://localhost:4040 (when running)

### Debug Mode

Enable debug logging by modifying `spark_utils.py`:

```python
_spark_session.sparkContext.setLogLevel("DEBUG")
```

## 📈 Scaling Up

For production use:

1. **Use a real Spark cluster** instead of local mode
2. **Optimize PostgreSQL** with proper indexing and configuration
3. **Monitor resources** and adjust Spark settings accordingly
4. **Consider partitioning** large tables by date or other criteria

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test with different data sizes
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License.

---

**Ready to process any amount of data with Spark and Airflow!** 🚀

### Comparison with Original Code

Your original code took **2hr 45min** for 1 billion records. This Spark solution should be **2-8x faster** depending on your system resources, potentially completing in **30-90 minutes** for the same billion records.
