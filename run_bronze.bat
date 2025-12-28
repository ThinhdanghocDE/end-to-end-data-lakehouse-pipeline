@echo off
echo ============================================
echo Starting BRONZE Streaming Job
echo ============================================
set PYSPARK_PYTHON=C:\Users\Admin\AppData\Local\Programs\Python\Python310\python.exe
set PYSPARK_DRIVER_PYTHON=C:\Users\Admin\AppData\Local\Programs\Python\Python310\python.exe
spark-submit ^
--packages ^
io.delta:delta-spark_2.12:3.2.0,^
org.apache.hadoop:hadoop-aws:3.3.4,^
com.amazonaws:aws-java-sdk-bundle:1.12.367,^
org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 ^
spark/streaming_bronze.py

pause
