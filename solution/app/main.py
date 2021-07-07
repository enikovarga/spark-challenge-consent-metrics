import argparse
import glob
import logging
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (avg, from_json, round, row_number, size,
                                   sum, when)
from pyspark.sql.window import Window


def create_spark_session():
    """Creating and configuring the spark session"""
    spark = SparkSession.builder.master("local[*]").appName("DidomiDataTest").getOrCreate()
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    return spark


def get_partitions(env, input_path):
    """
    Function to list all available partitions. Listing function can be made
    environment specific to support local and cloud execution.

    :param env: environment variable
    :param input_path: input data path
    :return: list of partition paths
    """
    if env == "dev":
        return glob.glob(f"{input_path}/*/")
    else:
        logging.info("Environment other than 'dev' is not yet supported")
        return []


def select_partitions_to_process(
    partitions, last_processed, dt_format="%Y-%m-%d-%H"
):
    """
    Function to evaluate which partitions has not been processed yet. It generates
    a string of all new partitions which is used to filter the dataset.

    :param partitions: list of partition paths
    :param last_processed: last successfully processed partition value
    :param dt_format: date format used for the partition
    :return: string of concatenated partition dates
    """
    filter_exp = ""

    for partition in partitions:
        dt = partition.split("=")[-1].strip("/")
        if datetime.strptime(dt, dt_format) > datetime.strptime(last_processed, dt_format):
            filter_exp += f"'{dt}',"

    return filter_exp.rstrip(",")


def parse_input_data(spark, input_path, filter_exp):
    """
    Function to read, filter and pre-process the dataset before calculating the metrics.
    It reads and filters the data to the required partitions, deduplicates the dataset and
    creates a set of flags for each record which will be used for metric calculation.

    :param spark: spark session
    :param input_path: input data path
    :param filter_exp: string of concatenated partition dates
    :return: dataframe
    """
    logging.info(f"Input data path: '{input_path}'")
    logging.info(f"Partition list: {filter_exp}")

    df = spark.read.json(input_path).filter(f"datehour IN ({filter_exp})")

    windowSpec = Window.partitionBy(df.id).orderBy(df.datetime.asc())

    df = df.withColumn("row_num", row_number().over(windowSpec)) \
            .filter("row_num = 1") \
            .drop("row_num")

    token_schema = spark.read.json(df.rdd.map(lambda row: row.user.token)).schema

    df = df.withColumn("user_token", from_json(df.user.token, token_schema))

    df = df.withColumn("pageviews_flag", when(df.type == "pageview", 1).otherwise(0)) \
        .withColumn("pageviews_with_consent_flag",
                    when((df.type == "pageview") & (size(df.user_token.purposes.enabled) > 0), 1).otherwise(0)) \
        .withColumn("consents_asked_flag", when(df.type == "consent.asked", 1).otherwise(0)) \
        .withColumn("consents_given_flag", when(df.type == "consent.given", 1).otherwise(0)) \
        .withColumn("consents_given_with_consent_flag",
                    when((df.type == "consent.given") & (size(df.user_token.purposes.enabled) > 0), 1).otherwise(0)) \
        .cache()

    return df


def compute_metrics(df):
    """
    Function to compute all metrics.

    :param df: dataframe of preprocessed data
    :return: a dataframe with the primary metrics and a dataframe with the user level metrics
    """
    primary_metrics_df = df.groupBy("datehour", "domain", "user.country").agg(
        sum("pageviews_flag").alias("pageviews"),
        sum("pageviews_with_consent_flag").alias("pageviews_with_consent"),
        sum("consents_asked_flag").alias("consents_asked"),
        sum("consents_given_flag").alias("consents_given"),
        sum("consents_given_with_consent_flag").alias("consents_given_with_consent"),
    )

    user_metrics_df = df.groupBy("datehour", "domain", "user.country", "user.id") \
        .agg(avg("pageviews_flag").alias("avg_pageviews")) \
        .withColumn("avg_pageviews_per_user", round("avg_pageviews", 2)) \
        .drop("avg_pageviews")

    return primary_metrics_df, user_metrics_df


def write_output(df, partition_col, output_path):
    """
    Function to persist the final output data.

    :param df: dataframe to be persisted
    :param partition_col: column name for partitioning
    :param output_path: output location
    """
    logging.info(f"Writing metrics to '{output_path}' partitioned by '{partition_col}'")
    df.coalesce(1).write.mode("overwrite").partitionBy(partition_col).json(output_path)


def main(args):
    spark = create_spark_session()
    partitions = get_partitions(args["env"], args["input_path"])
    partition_filter_exp = select_partitions_to_process(partitions, args["last_partition"])

    if len(partition_filter_exp) == 0:
        print("No data to process")
        exit(0)

    df = parse_input_data(spark, args["input_path"], partition_filter_exp)

    primary_metrics_df, user_metrics_df = compute_metrics(df)

    write_output(primary_metrics_df, "datehour", args["output_path"] + "/primary_metrics")
    write_output(user_metrics_df, "datehour", args["output_path"] + "/user_metrics")

    logging.info("Finished processing")

    spark.stop()


if __name__ == "__main__":
    try:
        logging.getLogger().setLevel(logging.INFO)

        parser = argparse.ArgumentParser(description="DataTest")
        parser.add_argument("--env", required=False, default="dev")
        parser.add_argument("--input_path", required=False, default="solution/input")
        parser.add_argument("--output_path", required=False, default="solution/output")
        parser.add_argument("--last_partition", required=False, default="1900-01-01-00")
        args = vars(parser.parse_args())

        main(args)

    except Exception as e:
        logging.error(e)
