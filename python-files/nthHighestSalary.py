from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col, dense_rank
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("getNthhighestSalary").master("local[*]").getOrCreate()

columns = ["id", "salary"]
data = [(1001, 25000), (1002, 36000), (1003, 75000), (1004, 86000), (1005, 15000), 
        (1006, 36000), (1007, 45000), (1008, 22000),(1009, 25000), (1010, 16000)]

df = spark.createDataFrame(data, columns)
df.show()

windowSpec = Window.partitionBy().orderBy(col("salary"))

df_salary_rank = df.withColumn("salary_rank", dense_rank().over(windowSpec))

df_salary_rank.show()

n = int(input("Enter the value of n: "))
print("n is: ", n)

df_salary_rank.filter(f"salary_rank == {n}").show()
