#!/usr/bin/env python3
"""
Script to create all remaining notebook files
Run this from the project root directory
"""

import os
import json
from pathlib import Path

def create_notebook_template(title, objectives):
    """Create a basic Jupyter notebook template."""
    notebook = {
        "cells": [
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": [
                    f"# Used Cars Analysis Pipeline - PySpark\n"
                    f"## {title}\n\n"
                    f"### Objectives:\n"
                    + "\n".join([f"{i+1}. {obj}" for i, obj in enumerate(objectives)])
                ]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "import sys\n"
                    "import os\n"
                    "import findspark\n"
                    "findspark.init()\n\n"
                    "from pyspark.sql import SparkSession\n"
                    "import warnings\n\n"
                    "project_root = os.path.abspath(os.path.join(os.getcwd(), '../../'))\n"
                    "sys.path.append(project_root)\n\n"
                    "from src.utils.spark_utils import create_spark_session, stop_spark_session\n\n"
                    "warnings.filterwarnings('ignore')\n\n"
                    "print(\"✅ Setup complete - Ready to create Spark session\")"
                ]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "# Create Spark session\n"
                    "spark = create_spark_session(\"PySpark Analysis Pipeline\")\n\n"
                    "print(\"\\n🔥 Spark session active!\")"
                ]
            },
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": [
                    "## TODO: Implement PySpark version of analysis\n\n"
                    "This notebook mirrors the Pandas implementation but uses PySpark for distributed processing.\n"
                ]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "# Stop Spark session when done\n"
                    "stop_spark_session(spark)"
                ]
            }
        ],
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3"
            }
        },
        "nbformat": 4,
        "nbformat_minor": 4
    }
    return notebook


# Define notebooks to create
pyspark_notebooks = {
    "01_data_profiling.ipynb": {
        "title": "01. Data Profiling and Exploration",
        "objectives": [
            "Load data using PySpark",
            "Generate data profiles",
            "Identify quality issues",
            "Compare performance with Pandas"
        ]
    },
    "02_data_cleaning.ipynb": {
        "title": "02. Data Cleaning and Preprocessing",
        "objectives": [
            "Handle missing values with PySpark",
            "Remove duplicates",
            "Standardize data types",
            "Measure processing time"
        ]
    },
    "03_quality_audit.ipynb": {
        "title": "03. Data Quality Audit",
        "objectives": [
            "Validate data quality",
            "Check business rules",
            "Generate quality scores",
            "Compare with Pandas results"
        ]
    },
    "04_time_series_modeling.ipynb": {
        "title": "04. Time Series Analysis",
        "objectives": [
            "Temporal aggregations with PySpark",
            "Trend analysis",
            "Distributed forecasting",
            "Performance benchmarking"
        ]
    },
    "05_enrichment_recommendations.ipynb": {
        "title": "05. Data Enrichment & Warehouse Design",
        "objectives": [
            "Enrichment strategies",
            "Star schema design",
            "ETL architecture",
            "Spark-based ETL patterns"
        ]
    }
}

def main():
    print("="*70)
    print("CREATING PROJECT NOTEBOOK FILES")
    print("="*70)
    
    # Get project root
    project_root = Path.cwd()
    
    # Create PySpark notebooks
    pyspark_dir = project_root / "notebooks" / "pyspark"
    pyspark_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"\n📁 Creating PySpark notebooks in: {pyspark_dir}")
    
    for filename, config in pyspark_notebooks.items():
        filepath = pyspark_dir / filename
        
        if filepath.exists():
            print(f"  ⏭️  {filename} already exists - skipping")
            continue
        
        notebook = create_notebook_template(config['title'], config['objectives'])
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(notebook, f, indent=2)
        
        print(f"  ✅ Created {filename}")
    
    # Create .gitkeep files
    print(f"\n📁 Creating .gitkeep files...")
    
    dirs_for_gitkeep = [
        "data/raw",
        "data/processed",
        "data/cleaned",
        "reports/figures",
        "reports/html"
    ]
    
    for dir_path in dirs_for_gitkeep:
        full_path = project_root / dir_path
        full_path.mkdir(parents=True, exist_ok=True)
        
        gitkeep_file = full_path / ".gitkeep"
        gitkeep_file.touch(exist_ok=True)
        print(f"  ✅ {dir_path}/.gitkeep")
    
    # Create __init__.py files
    print(f"\n📁 Creating __init__.py files...")
    
    init_dirs = [
        "src",
        "src/utils",
        "src/pandas_pipeline",
        "src/pyspark_pipeline"
    ]
    
    for dir_path in init_dirs:
        full_path = project_root / dir_path
        full_path.mkdir(parents=True, exist_ok=True)
        
        init_file = full_path / "__init__.py"
        if not init_file.exists():
            init_file.write_text("# Package initialization\n")
            print(f"  ✅ {dir_path}/__init__.py")
    
    print("\n" + "="*70)
    print("✅ ALL FILES CREATED SUCCESSFULLY!")
    print("="*70)
    print("\n📝 Next steps:")
    print("  1. Implement PySpark notebooks (mirror Pandas logic)")
    print("  2. Download datasets to data/raw/")
    print("  3. Run notebooks in order (01-05)")
    print("  4. Compare Pandas vs PySpark performance")
    print("\n🚀 Ready to start your analysis!")

if __name__ == "__main__":
    main()