from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from common.readdatautil import ReadDataUtil

if __name__ == '__main__':

    spark = SparkSession.builder.master("local[*]") \
        .appName("project2").getOrCreate()


    # rdu = ReadDataUtil()





    srcDF = spark.read.option("multiline", True).csv(r"C:\Users\SHREE\PycharmProjects\pythonProject2\Sourcefile\employee_project.csv", header=True, inferSchema=True)

# replace (\n) by (' ').
    srcDF = srcDF.withColumn("address", regexp_replace("address", r"\n", " "))
    # srcDF.show()
    # srcDF.printSchema()

    # Add Date.
    srcDF = srcDF.withColumn("fromdate", current_date())
    # srcDF.show()
    # srcDF.cache()

    # create empty dataframe for Target File

    schema = StructType([StructField("tgt_emp_id", StringType()),
                         StructField("tgt_fname", StringType()),
                         StructField("tgt_lname", StringType()),
                         StructField("tgt_age", IntegerType()),
                         StructField("tgt_salary", LongType()),
                         StructField("tgt_dept_id", IntegerType()),
                         StructField("tgt_address", StringType()),
                         StructField("tgt_city", StringType()),
                         StructField("tgt_state", StringType()),
                         StructField("tgt_mobile_number", DoubleType()),
                         StructField("tgt_fromdate", DateType())
                         ])
    TargetDF = spark.createDataFrame([], schema=schema)
    # TargetDF.show()

    # left outer join on source and target
    emp_df = srcDF.join(TargetDF, srcDF.emp_id == TargetDF.tgt_emp_id, 'left')

    emp_df.show()


# flag the record for insert or update

# insert flag

    scd_df = emp_df.withColumn("insertflag", when((emp_df.emp_id != emp_df.tgt_emp_id) |
                               emp_df.emp_id.isNull(), 'Y').otherwise('NA'))

    scd_df.show()

# update flag

    # scd_df1 = scd_df.withColumn("updateflag", when(scd_df.emp_id == scd_df.tgt_emp_id,'Y')
    #                             .otherwise('NA'))
    # scd_df1.show()








# save file in TargetFile (SCD 0 means No Change).
#     empDF.write.csv(r"C:\Users\SHREE\PycharmProjects\pythonProject2\TargetFile\employee_project1.csv")

