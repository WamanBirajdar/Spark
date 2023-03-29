# read csv file from hadoop

df=spark.read.option("header"=True).csv("MENTION YOUR YOUR CSV FILE PATH")

# here we have one issue in above command all fileds/columns dataype showing us as string to overcome this problem use below command
# you can use diff property for infer spark will identiy automatically spark will identified by itself

df=spark.read.option("header"=True).option("inferSchema"=True).csv("MENTION HERE YOUR HDFS PATH")

#print data
df.pritSchema()


#In manager id have some dirty data replace "-" with null and apply explicilty dataype



