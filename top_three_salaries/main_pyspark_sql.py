from pyspark.sql import SparkSession
from pyspark.testing.utils import assertDataFrameEqual


def run():
    spark.read.csv("data/employees.csv", header=True, inferSchema=True).createOrReplaceTempView("employees")
    spark.read.csv("data/departments.csv", header=True, inferSchema=True).createOrReplaceTempView("departments")
    actual_df = spark.sql("""
        WITH result AS (
            SELECT
                departments.name AS department,
                employees.name AS employee,
                salary AS salary,
                DENSE_RANK() OVER (PARTITION BY department_id ORDER BY salary DESC) AS rank
            FROM employees
            JOIN departments ON employees.department_id = departments.id
        )

        SELECT department, employee, salary FROM result
        WHERE
            rank <= 3
    """)

    expected_df = spark.read.csv("data/result.csv", header=True, inferSchema=True)
    assertDataFrameEqual(actual_df, expected_df)
    print("Success!")


if __name__ == "__main__":
    spark = spark = SparkSession.builder.appName("TopThreeSalaries").getOrCreate()
    run()
    spark.stop()
