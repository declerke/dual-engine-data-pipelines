# src/utils/spark_utils.py
"""
Spark Session Initialization and Utilities
Handles the Windows-specific configuration we fought so hard to stabilize
"""

import os
import findspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import yaml


def load_config(config_path="config/pipeline_config.yaml"):
    """Load pipeline configuration from YAML."""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def create_spark_session(app_name=None, config_overrides=None):
    """
    Create and configure Spark Session with Windows-optimized settings.
    
    Args:
        app_name (str): Application name for Spark UI
        config_overrides (dict): Additional Spark configurations
    
    Returns:
        SparkSession: Configured Spark session
    """
    # Initialize findspark
    findspark.init()
    
    # Load configuration
    config = load_config()
    spark_config = config.get('spark', {})
    
    if app_name is None:
        app_name = spark_config.get('app_name', 'DataPipeline')
    
    # Build Spark configuration
    conf = SparkConf()
    conf.setAppName(app_name)
    conf.setMaster(spark_config.get('master', 'local[*]'))
    
    # Memory settings
    memory = spark_config.get('memory', {})
    conf.set('spark.driver.memory', memory.get('driver', '4g'))
    conf.set('spark.executor.memory', memory.get('executor', '4g'))
    
    # Apply predefined configurations
    for key, value in spark_config.get('config', []):
        conf.set(key, value)
    
    # Apply overrides
    if config_overrides:
        for key, value in config_overrides.items():
            conf.set(key, value)
    
    # CRITICAL: Windows-specific IPv4 enforcement
    # This fixes the SocketTimeoutException we battled
    conf.set('spark.driver.host', '127.0.0.1')
    conf.set('spark.driver.bindAddress', '127.0.0.1')
    
    # Create session
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    
    # Set log level to reduce noise
    spark.sparkContext.setLogLevel('WARN')
    
    print(f"✅ Spark Session Created: {app_name}")
    print(f"   Spark Version: {spark.version}")
    print(f"   Master: {spark.sparkContext.master}")
    print(f"   Driver Memory: {spark.conf.get('spark.driver.memory')}")
    
    return spark


def stop_spark_session(spark):
    """Gracefully stop Spark session."""
    if spark:
        spark.stop()
        print("✅ Spark Session Stopped")


def get_spark_ui_url(spark):
    """Get Spark UI URL for monitoring."""
    return spark.sparkContext.uiWebUrl


def display_spark_config(spark):
    """Display current Spark configuration."""
    print("\n" + "="*70)
    print("SPARK CONFIGURATION")
    print("="*70)
    
    important_configs = [
        'spark.app.name',
        'spark.master',
        'spark.driver.memory',
        'spark.executor.memory',
        'spark.sql.adaptive.enabled',
        'spark.serializer',
        'spark.driver.host'
    ]
    
    for config in important_configs:
        try:
            value = spark.conf.get(config)
            print(f"{config:<40} = {value}")
        except:
            pass
    
    print("="*70 + "\n")


# Example usage and testing
if __name__ == "__main__":
    # Test Spark session creation
    spark = create_spark_session("TestSession")
    
    # Display configuration
    display_spark_config(spark)
    
    # Show Spark UI URL
    print(f"Spark UI: {get_spark_ui_url(spark)}")
    
    # Test with sample data
    data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
    df = spark.createDataFrame(data, ["name", "age"])
    
    print("\nTest DataFrame:")
    df.show()
    
    print(f"\nDataFrame count: {df.count()}")
    
    # Stop session
    stop_spark_session(spark)
    
    print("\n✅ Spark utilities test completed successfully!")