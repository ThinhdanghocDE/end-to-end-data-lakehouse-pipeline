@echo off
set PYSPARK_PYTHON=C:\Users\Admin\AppData\Local\Programs\Python\Python310\python.exe
set PYSPARK_DRIVER_PYTHON=C:\Users\Admin\AppData\Local\Programs\Python\Python310\python.exe
call run_silver.bat
call run_gold.bat
call run_warehouse.bat
