#import sparksession method from class pyspark.sql 
from pyspark.sql import SparkSession

#also importing some dataypes from pyspark 
from pyspark.sql.types import StructType,StructField, StringType, IntegerType

# creating spark session here with spark object for further computation
spark = SparkSession.builder.master("spark://localhost:7077").appName("demo").getOrCreate()


#In pyspark reading department file from hdfs 
depDf=spark.read.option("header"=True).option("inferSchema"=True).csv("MENTION_YOUR_HDFS_PATH/departments.csv")

#To check shcema of dataframe use printSchema()
depDf.printSchema()
# Here is output
root
 |-- DEPARTMENT_ID: string (nullable = true)
 |-- DEPARTMENT_NAME: string (nullable = true)
 |-- MANAGER_ID: string (nullable = true)
 |-- LOCATION_ID: string (nullable = true)

#due to dirty data spark can't identify datatype of fields it consider all integers fields as string to overcome this problem use inferSchema forcefully hadle
depDf=spark.read().option("header",True).option("inferSchema",True).csv("MENTION_YOUR_HDFS_PATH/departmetns.csv")
#output
depDf.printSchema()
root
 |-- DEPARTMENT_ID: integer (nullable = true)
 |-- DEPARTMENT_NAME: string (nullable = true)
 |-- MANAGER_ID: string (nullable = true)
 |-- LOCATION_ID: integer (nullable = true)
  
#Access all record from dataframe  
depDf.select("*").show()

#output
+-------------+--------------------+----------+-----------+
|DEPARTMENT_ID|     DEPARTMENT_NAME|MANAGER_ID|LOCATION_ID|
+-------------+--------------------+----------+-----------+
|           10|      Administration|       200|       1700|
|           20|           Marketing|       201|       1800|
|           30|          Purchasing|       114|       1700|
|           40|     Human Resources|       203|       2400|
|           50|            Shipping|       121|       1500|
|           60|                  IT|       103|       1400|
|           70|    Public Relations|       204|       2700|
|           80|               Sales|       145|       2500|
|           90|           Executive|       100|       1700|
|          100|             Finance|       108|       1700|
|          110|          Accounting|       205|       1700|
|          120|            Treasury|        - |       1700|
|          130|       Corporate Tax|        - |       1700|
|          140|  Control And Credit|        - |       1700|
|          150|Shareholder Services|        - |       1700|
|          160|            Benefits|        - |       1700|
|          170|       Manufacturing|        - |       1700|
|          180|        Construction|        - |       1700|
|          190|         Contracting|        - |       1700|
|          200|          Operations|        - |       1700|
+-------------+--------------------+----------+-----------+

#diff ways of retrive column from tabel

#1.method
depDf.select(depDf.DEPARTMENT_ID,depDf.DEPARTMENT_NAME).show()
#2.method
depDf.select("DEPARTMENT_ID","DEPARTMENT_NAME").show()
#3.method
depDf.select(depDf["DEPARTMENT_ID"],depDf["DEPARTMENT_NAME"]).show()
#ouput of above all command is same
+-------------+--------------------+
|DEPARTMENT_ID|     DEPARTMENT_NAME|
+-------------+--------------------+
|           10|      Administration|
|           20|           Marketing|
|           30|          Purchasing|
|           40|     Human Resources|
|           50|            Shipping|
|           60|                  IT|
|           70|    Public Relations|
|           80|               Sales|
|           90|           Executive|
|          100|             Finance|
|          110|          Accounting|
|          120|            Treasury|
|          130|       Corporate Tax|
|          140|  Control And Credit|
|          150|Shareholder Services|
|          160|            Benefits|
|          170|       Manufacturing|
|          180|        Construction|
|          190|         Contracting|
|          200|          Operations|
+-------------+--------------------+


#4.method
from pyspark.sql.functions import col
depDf.select(col("DEPARTMENT_ID").alias("DEP_ID"),col("DEPARTMENT_NAME").alias("DEP_NAME")).show()


#ouput
+------+--------------------+
|EMP_ID|              Dep_id|
+------+--------------------+
|    10|      Administration|
|    20|           Marketing|
|    30|          Purchasing|
|    40|     Human Resources|
|    50|            Shipping|
|    60|                  IT|
|    70|    Public Relations|
|    80|               Sales|
|    90|           Executive|
|   100|             Finance|
|   110|          Accounting|
|   120|            Treasury|
|   130|       Corporate Tax|
|   140|  Control And Credit|
|   150|Shareholder Services|
|   160|            Benefits|
|   170|       Manufacturing|
|   180|        Construction|
|   190|         Contracting|
|   200|          Operations|
+------+--------------------+


#HERE WE TRYING TO DERIVED NEW COLUMN FROM EXISTING COLUMN
empDf.select(col("EMPLOYEE_ID"),col("FIRST_NAME"),col("SALARY")).withColumn("NEW_SALARY",col("salary")+1000).show()

#OUPUT
+-----------+----------+------+----------+
|EMPLOYEE_ID|FIRST_NAME|SALARY|NEW_SALARY|
+-----------+----------+------+----------+
|        198|    Donald|  2600|    3600.0|
|        199|   Douglas|  2600|    3600.0|
|        200|  Jennifer|  4400|    5400.0|
|        201|   Michael| 13000|   14000.0|
|        202|       Pat|  6000|    7000.0|
|        203|     Susan|  6500|    7500.0|
|        204|   Hermann| 10000|   11000.0|
|        205|   Shelley| 12008|   13008.0|
|        206|   William|  8300|    9300.0|
|        100|    Steven| 24000|   25000.0|
|        101|     Neena| 17000|   18000.0|
|        102|       Lex| 17000|   18000.0|
|        103| Alexander|  9000|   10000.0|
|        104|     Bruce|  6000|    7000.0|
|        105|     David|  4800|    5800.0|
|        106|     Valli|  4800|    5800.0|
|        107|     Diana|  4200|    5200.0|
|        108|     Nancy| 12008|   13008.0|
|        109|    Daniel|  9000|   10000.0|
|        110|      John|  8200|    9200.0|
+-----------+----------+------+----------+


#UPDATE EXISTING COLUMN
empDf.withColumn("NEW_SALARY",col("SALARY")-1000).select("EMPLOYEE_ID","FIRST_NAME","NEW_SALARY").show()

#OUPUT
+-----------+----------+----------+
|EMPLOYEE_ID|FIRST_NAME|NEW_SALARY|
+-----------+----------+----------+
|        198|    Donald|    1600.0|
|        199|   Douglas|    1600.0|
|        200|  Jennifer|    3400.0|
|        201|   Michael|   12000.0|
|        202|       Pat|    5000.0|
|        203|     Susan|    5500.0|
|        204|   Hermann|    9000.0|
|        205|   Shelley|   11008.0|
|        206|   William|    7300.0|
|        100|    Steven|   23000.0|
|        101|     Neena|   16000.0|
|        102|       Lex|   16000.0|
|        103| Alexander|    8000.0|
|        104|     Bruce|    5000.0|
|        105|     David|    3800.0|
|        106|     Valli|    3800.0|
|        107|     Diana|    3200.0|
|        108|     Nancy|   11008.0|
|        109|    Daniel|    8000.0|
|        110|      John|    7200.0|
+-----------+----------+----------+


#UPDATE COLUMN NAME OR RENAME
empDf.withColumnRenamed("SALARY","EMP_SALARY").show()

#OUTPUT
+-----------+----------+---------+--------+------------+---------+----------+----------+--------------+----------+-------------+
|EMPLOYEE_ID|FIRST_NAME|LAST_NAME|   EMAIL|PHONE_NUMBER|HIRE_DATE|    JOB_ID|EMP_SALARY|COMMISSION_PCT|MANAGER_ID|DEPARTMENT_ID|
+-----------+----------+---------+--------+------------+---------+----------+----------+--------------+----------+-------------+
|        198|    Donald| OConnell|DOCONNEL|650.507.9833|21-JUN-07|  SH_CLERK|      2600|            - |       124|           50|
|        199|   Douglas|    Grant|  DGRANT|650.507.9844|13-JAN-08|  SH_CLERK|      2600|            - |       124|           50|
|        200|  Jennifer|   Whalen| JWHALEN|515.123.4444|17-SEP-03|   AD_ASST|      4400|            - |       101|           10|
|        201|   Michael|Hartstein|MHARTSTE|515.123.5555|17-FEB-04|    MK_MAN|     13000|            - |       100|           20|
|        202|       Pat|      Fay|    PFAY|603.123.6666|17-AUG-05|    MK_REP|      6000|            - |       201|           20|
|        203|     Susan|   Mavris| SMAVRIS|515.123.7777|07-JUN-02|    HR_REP|      6500|            - |       101|           40|
|        204|   Hermann|     Baer|   HBAER|515.123.8888|07-JUN-02|    PR_REP|     10000|            - |       101|           70|
|        205|   Shelley|  Higgins|SHIGGINS|515.123.8080|07-JUN-02|    AC_MGR|     12008|            - |       101|          110|
|        206|   William|    Gietz|  WGIETZ|515.123.8181|07-JUN-02|AC_ACCOUNT|      8300|            - |       205|          110|
|        100|    Steven|     King|   SKING|515.123.4567|17-JUN-03|   AD_PRES|     24000|            - |        - |           90|
|        101|     Neena|  Kochhar|NKOCHHAR|515.123.4568|21-SEP-05|     AD_VP|     17000|            - |       100|           90|
|        102|       Lex|  De Haan| LDEHAAN|515.123.4569|13-JAN-01|     AD_VP|     17000|            - |       100|           90|
|        103| Alexander|   Hunold| AHUNOLD|590.423.4567|03-JAN-06|   IT_PROG|      9000|            - |       102|           60|
|        104|     Bruce|    Ernst|  BERNST|590.423.4568|21-MAY-07|   IT_PROG|      6000|            - |       103|           60|
|        105|     David|   Austin| DAUSTIN|590.423.4569|25-JUN-05|   IT_PROG|      4800|            - |       103|           60|
|        106|     Valli|Pataballa|VPATABAL|590.423.4560|05-FEB-06|   IT_PROG|      4800|            - |       103|           60|
|        107|     Diana|  Lorentz|DLORENTZ|590.423.5567|07-FEB-07|   IT_PROG|      4200|            - |       103|           60|
|        108|     Nancy|Greenberg|NGREENBE|515.124.4569|17-AUG-02|    FI_MGR|     12008|            - |       101|          100|
|        109|    Daniel|   Faviet| DFAVIET|515.124.4169|16-AUG-02|FI_ACCOUNT|      9000|            - |       108|          100|
|        110|      John|     Chen|   JCHEN|515.124.4269|28-SEP-05|FI_ACCOUNT|      8200|            - |       108|          100|
+-----------+----------+---------+--------+------------+---------+----------+----------+--------------+----------+-------------+
only showing top 20 rows


#DROP COLUMN FROM DATAFRAME
empDf.drop("COMMISSION_PCT").show()
+-----------+----------+---------+--------+------------+---------+----------+------+----------+-------------+
|EMPLOYEE_ID|FIRST_NAME|LAST_NAME|   EMAIL|PHONE_NUMBER|HIRE_DATE|    JOB_ID|SALARY|MANAGER_ID|DEPARTMENT_ID|
+-----------+----------+---------+--------+------------+---------+----------+------+----------+-------------+
|        198|    Donald| OConnell|DOCONNEL|650.507.9833|21-JUN-07|  SH_CLERK|  2600|       124|           50|
|        199|   Douglas|    Grant|  DGRANT|650.507.9844|13-JAN-08|  SH_CLERK|  2600|       124|           50|
|        200|  Jennifer|   Whalen| JWHALEN|515.123.4444|17-SEP-03|   AD_ASST|  4400|       101|           10|
|        201|   Michael|Hartstein|MHARTSTE|515.123.5555|17-FEB-04|    MK_MAN| 13000|       100|           20|
|        202|       Pat|      Fay|    PFAY|603.123.6666|17-AUG-05|    MK_REP|  6000|       201|           20|
|        203|     Susan|   Mavris| SMAVRIS|515.123.7777|07-JUN-02|    HR_REP|  6500|       101|           40|
|        204|   Hermann|     Baer|   HBAER|515.123.8888|07-JUN-02|    PR_REP| 10000|       101|           70|
|        205|   Shelley|  Higgins|SHIGGINS|515.123.8080|07-JUN-02|    AC_MGR| 12008|       101|          110|
|        206|   William|    Gietz|  WGIETZ|515.123.8181|07-JUN-02|AC_ACCOUNT|  8300|       205|          110|
|        100|    Steven|     King|   SKING|515.123.4567|17-JUN-03|   AD_PRES| 24000|        - |           90|
|        101|     Neena|  Kochhar|NKOCHHAR|515.123.4568|21-SEP-05|     AD_VP| 17000|       100|           90|
|        102|       Lex|  De Haan| LDEHAAN|515.123.4569|13-JAN-01|     AD_VP| 17000|       100|           90|
|        103| Alexander|   Hunold| AHUNOLD|590.423.4567|03-JAN-06|   IT_PROG|  9000|       102|           60|
|        104|     Bruce|    Ernst|  BERNST|590.423.4568|21-MAY-07|   IT_PROG|  6000|       103|           60|
|        105|     David|   Austin| DAUSTIN|590.423.4569|25-JUN-05|   IT_PROG|  4800|       103|           60|
|        106|     Valli|Pataballa|VPATABAL|590.423.4560|05-FEB-06|   IT_PROG|  4800|       103|           60|
|        107|     Diana|  Lorentz|DLORENTZ|590.423.5567|07-FEB-07|   IT_PROG|  4200|       103|           60|
|        108|     Nancy|Greenberg|NGREENBE|515.124.4569|17-AUG-02|    FI_MGR| 12008|       101|          100|
|        109|    Daniel|   Faviet| DFAVIET|515.124.4169|16-AUG-02|FI_ACCOUNT|  9000|       108|          100|
|        110|      John|     Chen|   JCHEN|515.124.4269|28-SEP-05|FI_ACCOUNT|  8200|       108|          100|
+-----------+----------+---------+--------+------------+---------+----------+------+----------+-------------+
only showing top 20 rows

#FILTER 
empDf.select("EMPLOYEE_ID","FIRST_NAME","SALARY").filter(col("SALARY")==2600).show()
+-----------+----------+------+
|EMPLOYEE_ID|FIRST_NAME|SALARY|
+-----------+----------+------+
|        198|    Donald|  2600|
|        199|   Douglas|  2600|
|        118|       Guy|  2600|
+-----------+----------+------+


#filter with and
empDf.select("EMPLOYEE_ID","FIRST_NAME","SALARY","DEPARTMENT_ID").filter((col("SALARY")==2600)&(col("EMPLOYEE_ID")==199)).show()
+-----------+----------+------+-------------+
|EMPLOYEE_ID|FIRST_NAME|SALARY|DEPARTMENT_ID|
+-----------+----------+------+-------------+
|        199|   Douglas|  2600|           50|
+-----------+----------+------+-------------+


# distinct values remove duplicates
empDf.distinct().show()
+-----------+-----------+-----------+--------+------------+---------+----------+------+--------------+----------+-------------+
|EMPLOYEE_ID| FIRST_NAME|  LAST_NAME|   EMAIL|PHONE_NUMBER|HIRE_DATE|    JOB_ID|SALARY|COMMISSION_PCT|MANAGER_ID|DEPARTMENT_ID|
+-----------+-----------+-----------+--------+------------+---------+----------+------+--------------+----------+-------------+
|        126|      Irene|Mikkilineni|IMIKKILI|650.124.1224|28-SEP-06|  ST_CLERK|  2700|            - |       120|           50|
|        138|    Stephen|     Stiles| SSTILES|650.121.2034|26-OCT-05|  ST_CLERK|  3200|            - |       123|           50|
|        123|     Shanta|    Vollman|SVOLLMAN|650.123.4234|10-OCT-05|    ST_MAN|  6500|            - |       100|           50|
|        104|      Bruce|      Ernst|  BERNST|590.423.4568|21-MAY-07|   IT_PROG|  6000|            - |       103|           60|
|        134|    Michael|     Rogers| MROGERS|650.127.1834|26-AUG-06|  ST_CLERK|  2900|            - |       122|           50|
|        105|      David|     Austin| DAUSTIN|590.423.4569|25-JUN-05|   IT_PROG|  4800|            - |       103|           60|
|        108|      Nancy|  Greenberg|NGREENBE|515.124.4569|17-AUG-02|    FI_MGR| 12008|            - |       101|          100|
|        203|      Susan|     Mavris| SMAVRIS|515.123.7777|07-JUN-02|    HR_REP|  6500|            - |       101|           40|
|        113|       Luis|       Popp|   LPOPP|515.124.4567|07-DEC-07|FI_ACCOUNT|  6900|            - |       108|          100|
|        131|      James|     Marlow| JAMRLOW|650.124.7234|16-FEB-05|  ST_CLERK|  2500|            - |       121|           50|
|        125|      Julia|      Nayer|  JNAYER|650.124.1214|16-JUL-05|  ST_CLERK|  3200|            - |       120|           50|
|        112|Jose Manuel|      Urman| JMURMAN|515.124.4469|07-MAR-06|FI_ACCOUNT|  7800|            - |       108|          100|
|        204|    Hermann|       Baer|   HBAER|515.123.8888|07-JUN-02|    PR_REP| 10000|            - |       101|           70|
|        120|    Matthew|      Weiss|  MWEISS|650.123.1234|18-JUL-04|    ST_MAN|  8000|            - |       100|           50|
|        201|    Michael|  Hartstein|MHARTSTE|515.123.5555|17-FEB-04|    MK_MAN| 13000|            - |       100|           20|
|        107|      Diana|    Lorentz|DLORENTZ|590.423.5567|07-FEB-07|   IT_PROG|  4200|            - |       103|           60|
|        132|         TJ|      Olson| TJOLSON|650.124.8234|10-APR-07|  ST_CLERK|  2100|            - |       121|           50|
|        130|      Mozhe|   Atkinson|MATKINSO|650.124.6234|30-OCT-05|  ST_CLERK|  2800|            - |       121|           50|
|        119|      Karen| Colmenares|KCOLMENA|515.127.4566|10-AUG-07|  PU_CLERK|  2500|            - |       114|           30|
|        133|      Jason|     Mallin| JMALLIN|650.127.1934|14-JUN-04|  ST_CLERK|  3300|            - |       122|           50|
+-----------+-----------+-----------+--------+------------+---------+----------+------+--------------+----------+-------------+
only showing top 20 rows




# HERE PERFOMR SOME ANALYTICAL OPERATION LIKE SUM,COUNT,AVG,MIN,MAX


from pyspark.sql.functions import *
empDf.select(count("salary")).show()
+-------------+
|count(salary)|
+-------------+
|           50|
+-------------+

>>> empDf.select(count("salary").alias("total_count")).show()
+-----------+
|total_count|
+-----------+
|         50|
+-----------+

>>> empDf.select(max("salary").alias("maximum_salary")).show()
+--------------+
|maximum_salary|
+--------------+
|          9000|
+--------------+

>>> empDf.select(min("salary").alias("minimum_salary")).show()
+--------------+
|minimum_salary|
+--------------+
|         10000|
+--------------+

>>> empDf.select(avg("salary").alias("avg_salary")).show()
+----------+
|avg_salary|
+----------+
|   6182.32|
+----------+

>>> empDf.select(sum("salary").alias("summation_salary")).show()
+----------------+
|summation_salary|
+----------------+
|        309116.0|
+----------------+







