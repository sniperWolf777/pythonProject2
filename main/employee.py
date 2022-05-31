from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from common.readdatautil import ReadDataUtil

if __name__ == '__main__':

    spark = SparkSession.builder.master("local[*]") \
        .appName("project2").getOrCreate()


    # rdu = ReadDataUtil()





    empDF = spark.read.option("multiline", True).csv(r"C:\Users\SHREE\PycharmProjects\pythonProject2\Sourcefile\employee_project.csv", header=True, inferSchema=True)

# replace (\n) by (' ').
    empDF = empDF.withColumn("address", regexp_replace("address", r"\n", " "))
    # empDF.show()
    # empDF.printSchema()

    # Add Date.
    empDF = empDF.withColumn("fromdate", current_date())
    # empDF.show()
    # empDF.cache()

    # create empty dataframe for Target File

    schema = StructType([StructField("emp_id", StringType()),
                         StructField("fname", StringType()),
                         StructField("lname", StringType()),
                         StructField("age", IntegerType()),
                         StructField("salary", LongType()),
                         StructField("dept_id", IntegerType()),
                         StructField("address", StringType()),
                         StructField("city", StringType()),
                         StructField("state", StringType()),
                         StructField("mobile_number", DoubleType()),
                         StructField("fromdate", DateType())
                         ])
    TargetDF = spark.createDataFrame([], schema=schema)
    # TargetDF.show()

    # left outer join on source and target
    emp_df = empDF.join(TargetDF, empDF.emp_id == TargetDF.emp_id, 'left')

    emp_df.show()


# flag the record for insert or update

# insert flag

    scd_df = emp_df.withColumn("insertflag", when((emp_df.emp_id2 != emp_df.emp_id) |
                               emp_df.emp_id.isNull(), 'Y').otherwise('NA')).withColumn('emp_id2', col('emp_id').alias('emp_id2'))

    # scd_df.show()

# update flag

    # scd_df1 = scd_df.withColumn("updateflag", when(scd_df.emp_id == scd_df.emp_id,'Y')
    #                             .otherwise('NA'))
    # scd_df1.show()








# save file in TargetFile (SCD 0 means No Change).
#     empDF.write.csv(r"C:\Users\SHREE\PycharmProjects\pythonProject2\TargetFile\employee_project1.csv")

