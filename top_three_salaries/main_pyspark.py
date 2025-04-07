from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import dense_rank
from pyspark.testing.utils import assertDataFrameEqual


def run():
    spark = spark = SparkSession.builder.appName("TopThreeSalaries").getOrCreate()
    # 1) Read the employees and departments dataframes
    employees_df = spark.read.csv("data/employees.csv", header=True, inferSchema=True)
    departments_df = spark.read.csv("data/departments.csv", header=True, inferSchema=True)
    # 2) Join the dataframes on the department_id
    df = employees_df.join(departments_df, on=[employees_df.department_id == departments_df.id], how="inner")
    # 3) Add a rank column, partitioning by department_id and ordering by salary
    window = Window.partitionBy(departments_df.id).orderBy(employees_df.salary.desc())
    df = df.withColumn("rank", dense_rank().over(window))
    # 4) Keep only the Top 3 salaries by department
    df = df.filter(df.rank <= 3)
    df = df.select(
        departments_df.name.alias("department"),
        employees_df.name.alias("employee"),
        employees_df.salary.alias("salary"),
    )

    expected_df = spark.read.csv("data/result.csv", header=True, inferSchema=True)
    assertDataFrameEqual(df, expected_df)
    print("Success!")
    spark.stop()


if __name__ == "__main__":
    run()
