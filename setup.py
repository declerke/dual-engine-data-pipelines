#!/usr/bin/env python3
"""
Setup script for Used Cars Analysis Pipeline
Creates directory structure and verifies environment
"""

import os
import sys
from pathlib import Path


def create_directory_structure():
    """Create all necessary directories."""
    directories = [
        'data/raw',
        'data/processed',
        'data/cleaned',
        'notebooks/pandas',
        'notebooks/pyspark',
        'src/pandas_pipeline',
        'src/pyspark_pipeline',
        'src/utils',
        'reports/figures',
        'reports/html',
        'config',
        'tests'
    ]
    
    print("📁 Creating directory structure...")
    for directory in directories:
        Path(directory).mkdir(parents=True, exist_ok=True)
        
        # Create .gitkeep for empty directories
        if 'data' in directory or 'reports' in directory:
            gitkeep = Path(directory) / '.gitkeep'
            gitkeep.touch(exist_ok=True)
    
    print("✅ Directory structure created")


def verify_python_version():
    """Verify Python version is 3.11."""
    version = sys.version_info
    print(f"\n🐍 Python Version: {version.major}.{version.minor}.{version.micro}")
    
    if version.major == 3 and version.minor == 11:
        print("✅ Python 3.11 detected - perfect for PySpark!")
    elif version.major == 3 and version.minor >= 8:
        print("⚠️  Python 3.8+ detected - should work but 3.11 is recommended")
    else:
        print("❌ Python 3.8+ required")
        return False
    
    return True


def check_java():
    """Check if Java is installed."""
    print("\n☕ Checking Java installation...")
    
    try:
        import subprocess
        result = subprocess.run(['java', '-version'], 
                              capture_output=True, 
                              text=True,
                              timeout=5)
        
        if result.returncode == 0:
            # Java version is in stderr for some reason
            output = result.stderr if result.stderr else result.stdout
            print(f"✅ Java detected:")
            print(f"   {output.split(chr(10))[0]}")
            return True
        else:
            print("❌ Java not found - required for PySpark")
            print("   Install Java 11 or 17 from: https://adoptium.net/")
            return False
    except FileNotFoundError:
        print("❌ Java not found - required for PySpark")
        print("   Install Java 11 or 17 from: https://adoptium.net/")
        return False
    except Exception as e:
        print(f"⚠️  Could not verify Java: {e}")
        return None


def check_datasets():
    """Check if datasets are downloaded."""
    print("\n📊 Checking datasets...")
    
    datasets = {
        'Airbnb NYC': 'data/raw/AB_NYC_2019.csv',
        'Superstore Sales': 'data/raw/superstore_sales.csv'
    }
    
    missing = []
    for name, path in datasets.items():
        if Path(path).exists():
            size = Path(path).stat().st_size / (1024 * 1024)
            print(f"✅ {name}: Found ({size:.1f} MB)")
        else:
            print(f"❌ {name}: Not found")
            missing.append(name)
    
    if missing:
        print("\n📥 Missing datasets - Download from:")
        if 'Airbnb NYC' in missing:
            print("   • Airbnb: https://www.kaggle.com/datasets/dgomonov/new-york-city-airbnb-open-data")
        if 'Superstore Sales' in missing:
            print("   • Superstore: https://www.kaggle.com/datasets/bhanupratapbiswas/superstore-sales")
        return False
    
    return True


def verify_packages():
    """Verify required packages are installed."""
    print("\n📦 Checking required packages...")
    
    required = {
        'pandas': 'Data manipulation',
        'pyspark': 'Distributed processing',
        'matplotlib': 'Plotting',
        'seaborn': 'Statistical visualization',
        'findspark': 'Spark initialization'
    }
    
    missing = []
    for package, description in required.items():
        try:
            __import__(package)
            print(f"✅ {package:<15} - {description}")
        except ImportError:
            print(f"❌ {package:<15} - {description}")
            missing.append(package)
    
    if missing:
        print("\n💡 Install missing packages:")
        print("   pip install -r requirements.txt")
        print("   # or")
        print("   conda env create -f environment.yml")
        return False
    
    return True


def test_spark():
    """Test if PySpark can start."""
    print("\n⚡ Testing PySpark initialization...")
    
    try:
        import findspark
        findspark.init()
        
        from pyspark.sql import SparkSession
        
        spark = SparkSession.builder \
            .appName("SetupTest") \
            .master("local[1]") \
            .config("spark.driver.host", "127.0.0.1") \
            .config("spark.driver.bindAddress", "127.0.0.1") \
            .config("spark.driver.memory", "1g") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("ERROR")
        
        # Test with sample data
        data = [("Alice", 25), ("Bob", 30)]
        df = spark.createDataFrame(data, ["name", "age"])
        
        count = df.count()
        spark.stop()
        
        print(f"✅ PySpark working! Test DataFrame count: {count}")
        return True
        
    except Exception as e:
        print(f"❌ PySpark test failed: {e}")
        print("\n💡 Troubleshooting:")
        print("   1. Ensure Java is installed")
        print("   2. Set JAVA_HOME environment variable")
        print("   3. Check Python version (3.11 recommended)")
        return False


def main():
    """Run all setup checks."""
    print("="*70)
    print("USED CARS ANALYSIS PIPELINE - SETUP")
    print("="*70)
    
    # Create structure
    create_directory_structure()
    
    # Run checks
    checks = {
        'Python Version': verify_python_version(),
        'Java Installation': check_java(),
        'Required Packages': verify_packages(),
        'Datasets': check_datasets(),
        'PySpark Test': test_spark()
    }
    
    # Summary
    print("\n" + "="*70)
    print("SETUP SUMMARY")
    print("="*70)
    
    for check, result in checks.items():
        if result is True:
            status = "✅ PASS"
        elif result is False:
            status = "❌ FAIL"
        else:
            status = "⚠️  SKIP"
        print(f"{status:<12} {check}")
    
    all_pass = all(r is True or r is None for r in checks.values())
    
    if all_pass:
        print("\n🎉 Setup complete! Ready to start analysis.")
        print("\n📝 Next steps:")
        print("   1. Ensure datasets are in data/raw/")
        print("   2. Activate environment: conda activate spark-project")
        print("   3. Start Jupyter: jupyter notebook")
        print("   4. Open: notebooks/pandas/01_data_profiling.ipynb")
    else:
        print("\n⚠️  Some checks failed - please resolve issues above")
    
    print("\n" + "="*70)


if __name__ == "__main__":
    main()