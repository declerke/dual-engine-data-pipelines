from src.utils.spark_utils import create_spark_session
import time

spark = create_spark_session("UI_Test")

print("Spark UI is live at http://127.0.0.1:4040")
print("Press Ctrl+C to stop the session and close the UI.")

try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    spark.stop()
