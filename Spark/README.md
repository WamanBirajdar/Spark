# Spark Practical

### Import Below two files in PySpark 
+ Use departmetns.csv
+ And employees.csv

### Create SparkSession
> Importing spark method from pyspark class
+ from pyspark.sql import spark
> Here create session
+ spark = SparkSession.builder.master("spark://localhost:7077").appName("demo").getOrCreate()
> Start using spark object in your applicaiton

