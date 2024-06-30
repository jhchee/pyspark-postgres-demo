from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = (
    spark.read.format("jdbc")
    .option(
        "url", "jdbc:postgresql://postgres:5432/postgres"
    )
    .option("driver", "org.postgresql.Driver")
    .option("dbtable", "users")
    .option("user", "admin")
    .option("password", "password")
    .load()
)

df.show()
print(df.count())
