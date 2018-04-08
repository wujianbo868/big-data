#!/usr/bin/env python
# -*- coding: utf-8 -*-


# example of spark rdd handle
from pyspark.sql import SparkSession


def map_function(x):
    print x


def test_sort_rdd_value():
    df = spark.read.json("../../resources/people.json")
    df.show()
    # foo = df.rdd.map(lambda p: (p.name, p))
    # foo.foreach(lambda p: map_function(p))
    oldColumns = df.schema.names
    newColums = ['foo', 'boo']
    rename_df = reduce(lambda df, idx: df.withColumnRenamed(oldColumns[idx], newColums[idx]), xrange(len(newColums)),
                       df)
    rename_df.show()


if __name__ == "__main__":
    # $example on:init_session$
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    test_sort_rdd_value()
    spark.stop()
