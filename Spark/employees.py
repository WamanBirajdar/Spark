>>> emp=spark.read.option("inferSchema",True).option("header",True).option("delimiter",",").csv("/data/employees.csv")
>>> emp.printSchema()
root
 |-- EMPLOYEE_ID: integer (nullable = true)
 |-- FIRST_NAME: string (nullable = true)
 |-- LAST_NAME: string (nullable = true)
 |-- EMAIL: string (nullable = true)
 |-- PHONE_NUMBER: string (nullable = true)
 |-- HIRE_DATE: string (nullable = true)
 |-- JOB_ID: string (nullable = true)
 |-- SALARY: integer (nullable = true)
 |-- COMMISSION_PCT: string (nullable = true)
 |-- MANAGER_ID: string (nullable = true)
 |-- DEPARTMENT_ID: integer (nullable = true)
  
  
  # due to dirty data date column showing its dataype as a string
  # here we have to perform typecasting on hire_date column
  
 >>> emp.show()
+-----------+----------+---------+--------+------------+---------+----------+------+--------------+----------+-------------+
|EMPLOYEE_ID|FIRST_NAME|LAST_NAME|   EMAIL|PHONE_NUMBER|HIRE_DATE|    JOB_ID|SALARY|COMMISSION_PCT|MANAGER_ID|DEPARTMENT_ID|
+-----------+----------+---------+--------+------------+---------+----------+------+--------------+----------+-------------+
|        198|    Donald| OConnell|DOCONNEL|650.507.9833|21-JUN-07|  SH_CLERK|  2600|            - |       124|           50|
|        199|   Douglas|    Grant|  DGRANT|650.507.9844|13-JAN-08|  SH_CLERK|  2600|            - |       124|           50|
|        200|  Jennifer|   Whalen| JWHALEN|515.123.4444|17-SEP-03|   AD_ASST|  4400|            - |       101|           10|
|        201|   Michael|Hartstein|MHARTSTE|515.123.5555|17-FEB-04|    MK_MAN| 13000|            - |       100|           20|
|        202|       Pat|      Fay|    PFAY|603.123.6666|17-AUG-05|    MK_REP|  6000|            - |       201|           20|
|        203|     Susan|   Mavris| SMAVRIS|515.123.7777|07-JUN-02|    HR_REP|  6500|            - |       101|           40|
|        204|   Hermann|     Baer|   HBAER|515.123.8888|07-JUN-02|    PR_REP| 10000|            - |       101|           70|
|        205|   Shelley|  Higgins|SHIGGINS|515.123.8080|07-JUN-02|    AC_MGR| 12008|            - |       101|          110|
|        206|   William|    Gietz|  WGIETZ|515.123.8181|07-JUN-02|AC_ACCOUNT|  8300|            - |       205|          110|
|        100|    Steven|     King|   SKING|515.123.4567|17-JUN-03|   AD_PRES| 24000|            - |        - |           90|
|        101|     Neena|  Kochhar|NKOCHHAR|515.123.4568|21-SEP-05|     AD_VP| 17000|            - |       100|           90|
|        102|       Lex|  De Haan| LDEHAAN|515.123.4569|13-JAN-01|     AD_VP| 17000|            - |       100|           90|
|        103| Alexander|   Hunold| AHUNOLD|590.423.4567|03-JAN-06|   IT_PROG|  9000|            - |       102|           60|
|        104|     Bruce|    Ernst|  BERNST|590.423.4568|21-MAY-07|   IT_PROG|  6000|            - |       103|           60|
|        105|     David|   Austin| DAUSTIN|590.423.4569|25-JUN-05|   IT_PROG|  4800|            - |       103|           60|
|        106|     Valli|Pataballa|VPATABAL|590.423.4560|05-FEB-06|   IT_PROG|  4800|            - |       103|           60|
|        107|     Diana|  Lorentz|DLORENTZ|590.423.5567|07-FEB-07|   IT_PROG|  4200|            - |       103|           60|
|        108|     Nancy|Greenberg|NGREENBE|515.124.4569|17-AUG-02|    FI_MGR| 12008|            - |       101|          100|
|        109|    Daniel|   Faviet| DFAVIET|515.124.4169|16-AUG-02|FI_ACCOUNT|  9000|            - |       108|          100|
|        110|      John|     Chen|   JCHEN|515.124.4269|28-SEP-05|FI_ACCOUNT|  8200|            - |       108|          100|
+-----------+----------+---------+--------+------------+---------+----------+------+--------------+----------+-------------+
only showing top 20 rows



# belowe method impo for we can give aliase name to 
from pyspark.sql.functions import col
>>> emp.select(col("EMPLOYEE_ID"),col("FIRST_NAME")).show()
+-----------+----------+
|EMPLOYEE_ID|FIRST_NAME|
+-----------+----------+
|        198|    Donald|
|        199|   Douglas|
|        200|  Jennifer|
|        201|   Michael|
|        202|       Pat|
|        203|     Susan|
|        204|   Hermann|
|        205|   Shelley|
|        206|   William|
|        100|    Steven|
|        101|     Neena|
|        102|       Lex|
|        103| Alexander|
|        104|     Bruce|
|        105|     David|
|        106|     Valli|
|        107|     Diana|
|        108|     Nancy|
|        109|    Daniel|
|        110|      John|
+-----------+----------+
only showing top 20 rows

#example of alias column name

>>> emp.select(col("EMPLOYEE_ID").alias("emp_is")).show()
+------+
|emp_is|
+------+
|   198|
|   199|
|   200|
|   201|
|   202|
|   203|
|   204|
|   205|
|   206|
|   100|
|   101|
|   102|
|   103|
|   104|
|   105|
|   106|
|   107|
|   108|
|   109|
|   110|
+------+
only showing top 20 rows


# updating derived new column from existing column

>>> emp.select("EMPLOYEE_ID","FIRST_NAME","SALARY").withColumn("New Slaary",col("SALARY")+ 1000).show()
+-----------+----------+------+----------+
|EMPLOYEE_ID|FIRST_NAME|SALARY|New Slaary|
+-----------+----------+------+----------+
|        198|    Donald|  2600|      3600|
|        199|   Douglas|  2600|      3600|
|        200|  Jennifer|  4400|      5400|
|        201|   Michael| 13000|     14000|
|        202|       Pat|  6000|      7000|
|        203|     Susan|  6500|      7500|
|        204|   Hermann| 10000|     11000|
|        205|   Shelley| 12008|     13008|
|        206|   William|  8300|      9300|
|        100|    Steven| 24000|     25000|
|        101|     Neena| 17000|     18000|
|        102|       Lex| 17000|     18000|
|        103| Alexander|  9000|     10000|
|        104|     Bruce|  6000|      7000|
|        105|     David|  4800|      5800|
|        106|     Valli|  4800|      5800|
|        107|     Diana|  4200|      5200|
|        108|     Nancy| 12008|     13008|
|        109|    Daniel|  9000|     10000|
|        110|      John|  8200|      9200|
+-----------+----------+------+----------+
only showing top 20 rows


#withColumn numerical operation perform 
