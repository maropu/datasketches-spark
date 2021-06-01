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
|{2006-12-14 09:00:00, 2006-12-21 09:00:00}|[04 01 11 28 0C 00 07 00 AA 1D 00 00 00 00 00 0...|
|{2009-12-03 09:00:00, 2009-12-10 09:00:00}|[04 01 11 28 0C 00 05 00 9E 05 00 00 00 00 00 0...|
|{2009-10-22 09:00:00, 2009-10-29 09:00:00}|[04 01 11 28 0C 00 07 00 60 27 00 00 00 00 00 0...|
+------------------------------------------+--------------------------------------------------+
only showing top 3 rows

>>> df = summaries.where("window.start > '2007-06-01' and window.end < '2010-01-01'").selectExpr("approx_percentile_combine(summaries) merged")
>>> df.selectExpr("approx_percentile_estimate(merged, 0.95) percentile").show()
+----------+
|percentile|
+----------+
|      3.25|
+----------+

>>> df.selectExpr("approx_pmf_estimate(merged, 4) pmf").show(1, false)
+--------------------------------------------------------------------------------------+
|pmf                                                                                   |
+--------------------------------------------------------------------------------------+
|[0.9250280810398008, 0.07003322180158443, 0.004825778691690984, 1.1291846692380381E-4]|
+--------------------------------------------------------------------------------------+
```

## Frequent Item Sketches

A class of “Heavy Hitters” algorithms enables you to approximately identify the “heaviest”
or “most frequently occurring” items in an input column:

```
# This example uses the E-Commerce Data from UK retailer in the Kaggle data set:
# - https://www.kaggle.com/carrie1/ecommerce-data
>>> df = spark.read.format("csv").option("header", True).load("data.csv")
>>> df.selectExpr("count(Description)", "approx_count_distinct(Description)").show()
+------------------+----------------------------------+
|count(Description)|approx_count_distinct(Description)|
+------------------+----------------------------------+
|            540455|                              4361|
+------------------+----------------------------------+

>>> df.selectExpr("inline(approx_freqitems(Description))").show(false)
+----------------------------------+--------+
|item                              |estimate|
+----------------------------------+--------+
|WHITE HANGING HEART T-LIGHT HOLDER|2369    |
|REGENCY CAKESTAND 3 TIER          |2200    |
|JUMBO BAG RED RETROSPOT           |2159    |
|PARTY BUNTING                     |1752    |
|LUNCH BAG RED RETROSPOT           |1638    |
|SET OF 3 CAKE TINS PANTRY DESIGN  |1562    |
|ASSORTED COLOUR BIRD ORNAMENT     |1504    |
+----------------------------------+--------+
```

To pre-compute summaries for each group and estimate frequent items in some of them,
you can use similar functions to the quantile sketch ones:

```
>>> import pyspark.sql.functions as f
>>> summaries = df.groupBy("Country").agg(f.expr("approx_freqitems_accumulate(Description) As summaries"))
>>> summaries.show(3)
+---------+--------------------+
|  Country|           summaries|
+---------+--------------------+
|   Sweden|[04 01 0A 0A 09 0...|
|Singapore|[04 01 0A 0A 08 0...|
|  Germany|[04 01 0A 0A 0A 0...|
+---------+--------------------+
only showing top 3 rows

>>> df = summaries.where("Country IN ('United Kingdom', 'Germany', 'Spain')").selectExpr("approx_freqitems_combine(summaries) merged")
>>> df.selectExpr("inline(approx_freqitems_estimate(merged))").show(10, False)
+----------------------------------+--------+
|item                              |estimate|
+----------------------------------+--------+
|WHITE HANGING HEART T-LIGHT HOLDER|2292    |
|JUMBO BAG RED RETROSPOT           |2042    |
|REGENCY CAKESTAND 3 TIER          |1965    |
|PARTY BUNTING                     |1678    |
|LUNCH BAG RED RETROSPOT           |1488    |
|ASSORTED COLOUR BIRD ORNAMENT     |1442    |
|SET OF 3 CAKE TINS PANTRY DESIGN  |1437    |
|PAPER CHAIN KIT 50'S CHRISTMAS    |1310    |
|LUNCH BAG  BLACK SKULL.           |1309    |
|SPOTTY BUNTING                    |1307    |
+----------------------------------+--------+
```

## TODO

 - Supports the other sketch algorithms implemented in Apache DataSketches.
 - Checks performance differences between the built-in funtion and DataSketches ones.

## Bug reports

If you hit some bugs and requests, please leave some comments on [Issues](https://github.com/maropu/datasketches-spark/issues)
or Twitter([@maropu](http://twitter.com/#!/maropu)).

