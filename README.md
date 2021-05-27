[![License](http://img.shields.io/:license-Apache_v2-blue.svg)](https://github.com/maropu/datasketches-spark/blob/master/LICENSE)
[![Build and test](https://github.com/maropu/datasketches-spark/workflows/Build%20and%20test/badge.svg)](https://github.com/maropu/datasketches-spark/actions?query=workflow%3A%22Build+and+test%22)

This repository provies Apache DataSketches experimental adapters for Apache Spark.
Please visit [the main website](https://datasketches.apache.org/) for more DataSketches information.

## Quantile Sketches

In the same way as the built-in quantile estimation function (`approx_percentile`),
this plugin enalbes you to use the alternative function (`approx_percentile_ex`) that exploits
theoretically-meageable and very compact quantile sketches:

```
$ git clone https://github.com/maropu/datasketches-spark.git
$ cd datasketches-spark
$ ./bin/pyspark

Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 3.1.1
      /_/

Using Python version 3.6.8 (default, Dec 29 2018 19:04:46)
SparkSession available as 'spark'.
DataSketches APIs available as built-in functions.

# This example uses the Individual household electric power consumption Data Set in the UCI Machine Learning Repository:
# - https://archive.ics.uci.edu/ml/datasets/Individual+household+electric+power+consumption
>>> df = spark.read.format("csv").option("header", true).option("sep", ";").load("household_power_consumption.txt").selectExpr("to_date(Date, 'dd/MM/yyyy') AS Date", "CAST(Global_active_power AS double) Global_active_power")
>>> df.describe().show(5, False)
+-------+-------------------+
|summary|Global_active_power|
+-------+-------------------+
|count  |2049280            |
|mean   |1.0916150365005453 |
|stddev |1.0572941610939872 |
|min    |0.076              |
|max    |11.122             |
+-------+-------------------+

>>> df.selectExpr("percentile(Global_active_power, 0.95) exact", "approx_percentile(Global_active_power, 0.95) builtin", "approx_percentile_ex(Global_active_power, 0.95) datasketches").show()
+-----+-------+----------------+
|exact|builtin|    datasketches|
+-----+-------+----------------+
|3.264|  3.264|3.25600004196167|
+-----+-------+----------------+
```

Moreover, this plugin provies functionalities to accumulate quantile summaries in time intervals and
estimate quantile values in specific intervals later just like [the Snowflake built-in functions](https://docs.snowflake.com/en/user-guide/querying-approximate-percentile-values.html):

```
>>> import pyspark.sql.functions as f
>>> summaries = df.groupBy(f.window("Date", "1 week")).agg(f.expr("approx_percentile_accumulate(Global_active_power) AS summaries"))
>>> summaries.show(3, 50)
+------------------------------------------+--------------------------------------------------+
|                                    window|                                         summaries|
+------------------------------------------+--------------------------------------------------+
|{2006-12-14 09:00:00, 2006-12-21 09:00:00}|[4, 1, 17, 40, 12, 0, 7, 0, -86, 29, 0, 0, 0, 0...|
|{2008-03-13 09:00:00, 2008-03-20 09:00:00}|[4, 1, 17, 40, 12, 0, 7, 0, 96, 39, 0, 0, 0, 0,...|
|{2007-05-03 09:00:00, 2007-05-10 09:00:00}|[4, 1, 17, 40, 12, 0, 7, 0, 96, 39, 0, 0, 0, 0,...|
+------------------------------------------+--------------------------------------------------+
only showing top 3 rows

>>> df = summaries.where("window.start > '2007-06-01' and window.end < '2010-01-01'").selectExpr("approx_percentile_combine(summaries) merged")
>>> df.selectExpr("approx_percentile_estimate(merged, 0.95) percentile").show()
+----------+
|percentile|
+----------+
|      3.25|
+----------+
```

## TODO

 - Supports the other sketch algorithms implemented in Apache DataSketches.
 - Checks performance differences between the built-in funtion and DataSketches ones.

## Bug reports

If you hit some bugs and requests, please leave some comments on [Issues](https://github.com/maropu/datasketches-spark/issues)
or Twitter([@maropu](http://twitter.com/#!/maropu)).

