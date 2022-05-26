from pyspark.sql import SparkSession

if __name__ == '__main__':


    spark = SparkSession.builder.master("local[*]").appName("other").getOrCreate()


    Survey_df = spark.read.csv(r"C:\Users\SHREE\PycharmProjects\pythonProject2\other\survey.csv",header=True)

    # Survey_df.show(truncate=False)

    department_df = spark.read.csv(r"C:\Users\SHREE\PycharmProjects\pythonProject2\other\department.csv",header=True)

    # department_df.show(truncate=False)