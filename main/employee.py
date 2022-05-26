from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from common.readdatautil import ReadDataUtil

if __name__ == '__main__':

    spark = SparkSession.builder.master("local[*]") \
        .appName("project2").getOrCreate()


    # rdu = ReadDataUtil()





    empDF = spark.read.option("multiline", True).csv(r"C:\Users\SHREE\PycharmProjects\pythonProject2\files\employee_project.csv", header=True)


# Remove \n from the address column.
    empdf = empDF.withColumn('address', regexp_replace("address", r"\n"," ")).show(truncate=False)

    empdf.show()


# save file in TargetFile (SCD 0 means No Change).
#     empDF.write.csv(r"C:\Users\SHREE\PycharmProjects\pythonProject2\TargetFile\employee_project1.csv")

