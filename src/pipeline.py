from pyspark.sql import SparkSession, functions as f
from pyspark.sql.types import IntegerType
import sys

spark = SparkSession.builder \
    .master('local')\
    .getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")


def load_s3_csv(path):
    bucket = "s3a://nd-group"
    return spark.read.csv(path.format(bucket=bucket), header=True, inferSchema=True, sep=',')


def read_input():
    df_calendar = load_s3_csv('{bucket}/nike/raw/calendar.csv')
    df_product = load_s3_csv('{bucket}/nike/raw/product.csv')
    df_sales = load_s3_csv('{bucket}/nike/raw/sales.csv')
    df_store = load_s3_csv('{bucket}/nike/raw/store.csv')

    return df_calendar, df_product, df_sales, df_store


def format_input(df_calendar, df_product, df_sales, df_store):
    df_calendar = df_calendar \
        .withColumnRenamed("datecalendaryear", "year") \
        .withColumnRenamed("weeknumberofseason", "week") \
        .withColumnRenamed("datekey", "dateId") \
        .drop("datecalendarday")

    df_product = df_product \
        .withColumnRenamed("productid", "productId")

    df_sales = df_sales

    df_store = df_store\
        .withColumnRenamed("storeid", "storeId") \
        .drop("country")

    return df_calendar, df_product, df_sales, df_store


def generate_spine(df):
    df = df.select("year", "channel", "category", "gender", "division").distinct()
    df_week = spark.createDataFrame(list(range(1, 53)), IntegerType()).withColumnRenamed("value", "week")
    output = df.crossJoin(df_week)
    return output


def format_output(df):
    df = df.withColumn("uniqueKey",
                       f.upper(f.concat(f.lit("RY"),
                                        f.substring(f.col('year'), 3, 2),
                                        f.lit("_"),
                                        f.col("channel"),
                                        f.lit("_"),
                                        f.col("division"),
                                        f.lit("_"),
                                        f.col("gender"),
                                        f.lit("_"),
                                        f.col("category"),
                                        ))) \
        .withColumn("channel", f.upper(f.col("channel"))) \
        .withColumn("year", f.concat(f.lit("RY"), f.substring(f.col('year'), 3, 2))) \
        .withColumn("week_1", f.concat(f.lit("W"), f.col("week")))

    output = df.orderBy("week").groupBy('uniqueKey', 'division', 'gender', 'category', 'channel', 'year').agg(
        f.to_json(
            f.collect_list(
                f.create_map('week_1', 'netSales')
            )
        ).alias('Net Sales'),
        f.to_json(
            f.collect_list(
                f.create_map('week_1', 'salesUnits')
            )
        ).alias('Sales Units')
    )

    return output


if __name__ == '__main__':
    df_calendar, df_product, df_sales, df_store = read_input()
    df_calendar, df_product, df_sales, df_store = format_input(df_calendar, df_product, df_sales, df_store)
    df_sales = df_sales \
        .join(df_calendar, on="dateId", how="inner") \
        .join(df_product, on="productId", how="inner") \
        .join(df_store, on="storeId", how="inner").drop("dateId", "productId", "storeId", "saleId")

    df_sales = df_sales.groupBy("year", "channel", "category", "gender", "division", "week") \
        .agg(f.sum("netSales").alias("netSales"), f.sum("salesUnits").alias("salesUnits"))

    spine = generate_spine(df_sales)
    join_with_spine = spine.join(df_sales,
                                 on=["year", "channel", "category", "gender", "division", "week"],
                                 how="left") \
        .fillna(0)

    if len(sys.argv) == 3:
        year = sys.argv[1]
        week = sys.argv[2]

        join_with_spine = join_with_spine.filter((f.col("year") == year) & (f.col("week") == week))
        res_df = format_output(join_with_spine)
        res_df.coalesce(1).write.format("json").option("header", "true").mode(
            "overwrite"
        ).save(
            "s3a://nd-group/nike/output/consumption"
        )
    else:
        res_df = format_output(join_with_spine)
        res_df.coalesce(1).write.format("json").option("header", "true").mode(
            "overwrite"
        ).save(
            "s3a://nd-group/nike/output/consumption"
        )
