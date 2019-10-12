============================ Dataframe without option function ===========================

val salesData = spark.read.csv("/home/cloudera/RahulData/500000SalesRecords.csv").

val salesData = spark.read.csv("/home/cloudera/RahulData/500000SalesRecords.csv").take(3)

val salesData = spark.read.csv("/home/cloudera/RahulData/500000SalesRecords.csv").sort("_c13").take(3)

val salesData = spark.read.csv("/home/cloudera/RahulData/500000SalesRecords.csv").sort("_c13").explain

--This will give the defalult count of partition will be created while shuffle opration 
scala> spark.conf.get("spark.sql.shuffle.partitions")
res6: String = 200

--This will set the defalult count of partition 
scala> spark.conf.set("spark.sql.shuffle.partitions",10)

--This will get the updated count of partition 
scala> spark.conf.get("spark.sql.shuffle.partitions")
res8: String = 10


============================ Dataframe with option function ===========================

//We are creating Dataframe from csv file using option tag, option tag is used to set the properties e.g header(is first line is header or not)
val retailData = spark.read.option("inferSchema","true").option("header","true").csv("/home/cloudera/RahulData/500000SalesRecords.csv");

//this will sort the data
scala> val sortedRetailData = retailData.sort("Ship Date")

//This will take first 3 rows and show them in tabular form
scala> val sortedRetailData = retailData.sort("Ship Date").show(3)
+--------------------+-------+---------+-------------+--------------+----------+---------+---------+----------+----------+---------+-------------+----------+------------+
|              Region|Country|Item Type|Sales Channel|Order Priority|Order Date| Order ID|Ship Date|Units Sold|Unit Price|Unit Cost|Total Revenue|Total Cost|Total Profit|
+--------------------+-------+---------+-------------+--------------+----------+---------+---------+----------+----------+---------+-------------+----------+------------+
|       North America| Canada|  Clothes|       Online|             H|  1/1/2010|755581557| 1/1/2010|      2228|    109.28|    35.84|    243475.84|  79851.52|   163624.32|
|Middle East and N...|  Qatar|Baby Food|      Offline|             M|  1/1/2010|531122720| 1/1/2010|      9870|    255.28|   159.42|    2519613.6| 1573475.4|    946138.2|
|Middle East and N...|  Syria|     Meat|       Online|             H|  1/1/2010|905299460| 1/1/2010|      7083|    421.89|   364.69|   2988246.87|2583099.27|    405147.6|
+--------------------+-------+---------+-------------+--------------+----------+---------+---------+----------+----------+---------+-------------+----------+------------+
only showing top 3 rows

sortedRetailData: Unit = ()

//This will take first 3 rows and show them in array format
scala> val sortedRetailData = retailData.sort("Ship Date").take(3)
sortedRetailData: Array[org.apache.spark.sql.Row] = Array([Middle East and North Africa,Qatar,Baby Food,Offline,M,1/1/2010,531122720,1/1/2010,9870,255.28,159.42,2519613.6,1573475.4,946138.2], 
[North America,Canada,Clothes,Online,H,1/1/2010,755581557,1/1/2010,2228,109.28,35.84,243475.84,79851.52,163624.32], 
[Middle East and North Africa,Syria,Meat,Online,H,1/1/2010,905299460,1/1/2010,7083,421.89,364.69,2988246.87,2583099.27,405147.6])



