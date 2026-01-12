# src/utils/pandas_utils.py
"""
Pandas Helper Functions for Data Analysis Pipeline
"""

import pandas as pd
import numpy as np
import yaml
import os
from pathlib import Path


def load_config(config_path="config/pipeline_config.yaml"):
    """Load pipeline configuration from YAML."""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def load_dataset(dataset_name, config_path="config/pipeline_config.yaml"):
    """
    Load dataset using configuration.
    
    Args:
        dataset_name (str): 'airbnb' or 'superstore'
        config_path (str): Path to configuration file
    
    Returns:
        pd.DataFrame: Loaded dataset
    """
    config = load_config(config_path)
    
    raw_path = config['paths']['raw_data']
    dataset_config = config['datasets'][dataset_name]
    filename = dataset_config['filename']
    
    file_path = os.path.join(raw_path, filename)
    
    print(f"Loading {dataset_name} dataset from: {file_path}")
    
    try:
        df = pd.read_csv(file_path)
        print(f"✅ Loaded {len(df):,} rows and {len(df.columns)} columns")
        return df
    except FileNotFoundError:
        print(f"❌ Error: File not found at {file_path}")
        print(f"   Please ensure the dataset is downloaded and placed in {raw_path}/")
        return None
    except Exception as e:
        print(f"❌ Error loading dataset: {e}")
        return None


def get_dataset_info(df):
    """Get comprehensive information about the dataset."""
    info = {
        'shape': df.shape,
        'memory_usage_mb': df.memory_usage(deep=True).sum() / 1024**2,
        'dtypes': df.dtypes.value_counts().to_dict(),
        'missing_values': df.isnull().sum().sum(),
        'missing_percentage': (df.isnull().sum().sum() / (df.shape[0] * df.shape[1])) * 100,
        'duplicate_rows': df.duplicated().sum(),
        'numeric_columns': df.select_dtypes(include=[np.number]).columns.tolist(),
        'categorical_columns': df.select_dtypes(include=['object', 'category']).columns.tolist()
    }
    return info


def print_dataset_summary(df, dataset_name="Dataset"):
    """Print a formatted summary of the dataset."""
    info = get_dataset_info(df)
    
    print("\n" + "="*70)
    print(f"{dataset_name.upper()} SUMMARY")
    print("="*70)
    print(f"\n📊 Dimensions:")
    print(f"   • Rows: {info['shape'][0]:,}")
    print(f"   • Columns: {info['shape'][1]}")
    print(f"   • Memory: {info['memory_usage_mb']:.2f} MB")
    
    print(f"\n🔢 Data Types:")
    for dtype, count in info['dtypes'].items():
        print(f"   • {dtype}: {count} columns")
    
    print(f"\n❌ Missing Data:")
    print(f"   • Total missing cells: {info['missing_values']:,}")
    print(f"   • Missing percentage: {info['missing_percentage']:.2f}%")
    
    print(f"\n🔄 Duplicates:")
    print(f"   • Duplicate rows: {info['duplicate_rows']:,}")
    
    print(f"\n📋 Column Categories:")
    print(f"   • Numeric: {len(info['numeric_columns'])}")
    print(f"   • Categorical: {len(info['categorical_columns'])}")
    
    print("\n" + "="*70 + "\n")


def convert_to_datetime(df, column, format=None, errors='coerce'):
    """
    Convert column to datetime with error handling.
    
    Args:
        df (pd.DataFrame): DataFrame
        column (str): Column name
        format (str): Date format string
        errors (str): How to handle errors ('coerce', 'raise', 'ignore')
    
    Returns:
        pd.DataFrame: DataFrame with converted column
    """
    print(f"Converting '{column}' to datetime...")
    
    original_nulls = df[column].isnull().sum()
    
    try:
        df[column] = pd.to_datetime(df[column], format=format, errors=errors)
        new_nulls = df[column].isnull().sum()
        
        if new_nulls > original_nulls:
            print(f"   ⚠️  Warning: {new_nulls - original_nulls} values became NaT")
        
        print(f"   ✅ Converted to datetime")
        return df
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return df


def detect_outliers_iqr(df, column, multiplier=1.5):
    """
    Detect outliers using IQR method.
    
    Args:
        df (pd.DataFrame): DataFrame
        column (str): Column name
        multiplier (float): IQR multiplier (1.5 = standard)
    
    Returns:
        pd.Series: Boolean mask of outliers
    """
    Q1 = df[column].quantile(0.25)
    Q3 = df[column].quantile(0.75)
    IQR = Q3 - Q1
    
    lower_bound = Q1 - multiplier * IQR
    upper_bound = Q3 + multiplier * IQR
    
    outliers = (df[column] < lower_bound) | (df[column] > upper_bound)
    
    print(f"Outliers in '{column}':")
    print(f"   • Range: [{lower_bound:.2f}, {upper_bound:.2f}]")
    print(f"   • Count: {outliers.sum()} ({outliers.sum()/len(df)*100:.2f}%)")
    
    return outliers


def create_time_features(df, date_column):
    """
    Create time-based features from datetime column.
    
    Args:
        df (pd.DataFrame): DataFrame
        date_column (str): Name of datetime column
    
    Returns:
        pd.DataFrame: DataFrame with new time features
    """
    print(f"Creating time features from '{date_column}'...")
    
    df[f'{date_column}_year'] = df[date_column].dt.year
    df[f'{date_column}_month'] = df[date_column].dt.month
    df[f'{date_column}_day'] = df[date_column].dt.day
    df[f'{date_column}_dayofweek'] = df[date_column].dt.dayofweek
    df[f'{date_column}_quarter'] = df[date_column].dt.quarter
    df[f'{date_column}_is_weekend'] = df[date_column].dt.dayofweek.isin([5, 6]).astype(int)
    
    print(f"   ✅ Created 6 time features")
    
    return df


def save_dataframe(df, filename, output_dir, format='csv'):
    """
    Save DataFrame to file with proper path handling.
    
    Args:
        df (pd.DataFrame): DataFrame to save
        filename (str): Output filename
        output_dir (str): Output directory
        format (str): Output format ('csv', 'parquet')
    """
    os.makedirs(output_dir, exist_ok=True)
    
    filepath = os.path.join(output_dir, filename)
    
    if format == 'csv':
        df.to_csv(filepath, index=False)
    elif format == 'parquet':
        df.to_parquet(filepath, index=False)
    else:
        raise ValueError(f"Unsupported format: {format}")
    
    print(f"✅ Saved to: {filepath}")


# Example usage
if __name__ == "__main__":
    # Test configuration loading
    config = load_config()
    print("Configuration loaded successfully:")
    print(f"  Raw data path: {config['paths']['raw_data']}")
    
    # Test dataset loading (will fail if file doesn't exist, which is expected)
    print("\nTesting dataset loading...")
    df = load_dataset('airbnb')
    
    if df is not None:
        print_dataset_summary(df, "Airbnb NYC")
    else:
        print("Dataset not found - this is expected if you haven't downloaded it yet")
    
    print("\n✅ Pandas utilities test completed!")