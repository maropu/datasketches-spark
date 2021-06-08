[![License](http://img.shields.io/:license-Apache_v2-blue.svg)](https://github.com/maropu/datasketches-spark/blob/master/LICENSE)
[![Build and test](https://github.com/maropu/datasketches-spark/workflows/Build%20and%20test/badge.svg)](https://github.com/maropu/datasketches-spark/actions?query=workflow%3A%22Build+and+test%22)

This repository provies Apache DataSketches experimental adapters for Apache Spark.
Please visit [the main website](https://datasketches.apache.org/) for more DataSketches information.

## Quantile Sketches

Like the built-in percentile estimation function (`approx_percentile`),
this plugin enalbes you to use an alternative function (`approx_percentile_ex`) to estimate percentiles
in a theoretically-meageable and very compact way:

```
$ git clone https://github.com/maropu/datasketches-spark.git
$ cd datasketches-spark
$ ./bin/pyspark

Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 3.1.2
      /_/

Using Python version 3.6.8 (default, Dec 29 2018 19:04:46)
SparkSession available as 'spark'.
DataSketches APIs available as built-in functions.

# This example uses the individual household electric power consumption data set in the UCI Machine Learning Repository:
# - https://archive.ics.uci.edu/ml/datasets/Individual+household+electric+power+consumption
>>> df = spark.read.format("csv").option("header", True).option("sep", ";").load("household_power_consumption.txt").selectExpr("to_date(Date, 'dd/MM/yyyy') AS Date", "CAST(Global_active_power AS double) Global_active_power")
>>> df.show(5)
+----------+-------------------+
|      Date|Global_active_power|
+----------+-------------------+
|2006-12-16|              4.216|
|2006-12-16|               5.36|
|2006-12-16|              5.374|
|2006-12-16|              5.388|
|2006-12-16|              3.666|
+----------+-------------------+
only showing top 5 rows

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

>>> df.selectExpr("percentile(Global_active_power, 0.95) percentile", "approx_percentile(Global_active_power, 0.95) approx_percentile", "approx_percentile_ex(Global_active_power, 0.95) approx_percentile_ex").show()
+----------+-----------------+--------------------+
|percentile|approx_percentile|approx_percentile_ex|
+----------+-----------------+--------------------+
|     3.264|            3.264|                3.25|
+----------+-----------------+--------------------+
```

Moreover, this plugin provies functionalities to accumulate quantile summaries for each time interval and
estimate quantile values over specific intervals later just like [the Snowflake built-in functions](https://docs.snowflake.com/en/user-guide/querying-approximate-percentile-values.html):

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

# Correct percentile of the `Global_active_power` column
scala> df.where("Date between '2007-06-01' and '2010-01-01'").selectExpr("percentile(Global_active_power, 0.95) correct").show()
+-------+
|correct|
+-------+
|  3.236|
+-------+

# Estimated percentile of the `Global_active_power` column
>>> df = summaries.where("window.start > '2007-06-01' and window.end < '2010-01-01'").selectExpr("approx_percentile_combine(summaries) merged")
>>> df.selectExpr("approx_percentile_estimate(merged, 0.95) percentile").show()
+----------+
| estimated|
+----------+
|      3.25|
+----------+

>>> df.selectExpr("approx_pmf_estimate(merged, 4) pmf").show(1, False)
+--------------------------------------------------------------------------------------+
|pmf                                                                                   |
+--------------------------------------------------------------------------------------+
|[0.9250280810398008, 0.07003322180158443, 0.004825778691690984, 1.1291846692380381E-4]|
+--------------------------------------------------------------------------------------+
```

### Configurations

| Property Name | Default | Meaning |
| ---- | ---- | ---- |
| spark.sql.dataSketches.quantiles.sketchImpl | REQ | A sketch implementation used in quantile estimation functions. |
| spark.sql.dataSketches.quantiles.kll.k | 200 | Specifies the parameter `k` for the quantile sketch implementation named `KLL`, `KllFloatsSketch`. |
| spark.sql.dataSketches.quantiles.req.k | 12 | Specifies the parameter `k` for the quantile sketch implementation named `REQ`, `ReqSketch`. |
| spark.sql.dataSketches.quantiles.mergeable.k | 128 | Specifies the parameter `k` for the quantile sketch implementation named `MERGEABLE`, `DoubleSketch`. |

## Frequent Item Sketches

A class of “Heavy Hitters” algorithms enables you to approximately identify the “heaviest”
or “most frequently occurring” items in an input column:

```
# This example uses the e-commerce data from UK retailer in the Kaggle data set:
# - https://www.kaggle.com/carrie1/ecommerce-data
>>> df = spark.read.format("csv").option("header", True).load("data.csv").selectExpr("Country", "Description")
>>> df.show(5, False)
+--------------+-----------------------------------+
|Country       |Description                        |
+--------------+-----------------------------------+
|United Kingdom|WHITE HANGING HEART T-LIGHT HOLDER |
|United Kingdom|WHITE METAL LANTERN                |
|United Kingdom|CREAM CUPID HEARTS COAT HANGER     |
|United Kingdom|KNITTED UNION FLAG HOT WATER BOTTLE|
|United Kingdom|RED WOOLLY HOTTIE WHITE HEART.     |
+--------------+-----------------------------------+
only showing top 5 rows

>>> df.selectExpr("count(Description)", "approx_count_distinct(Description)").show()
+------------------+----------------------------------+
|count(Description)|approx_count_distinct(Description)|
+------------------+----------------------------------+
|            540455|                              4361|
+------------------+----------------------------------+

# Correct item counts of the `Description` column
>>> df.groupBy("Description").count().orderBy(col("count").desc()).show(7, False)
+----------------------------------+-----+
|Description                       |count|
+----------------------------------+-----+
|WHITE HANGING HEART T-LIGHT HOLDER|2369 |
|REGENCY CAKESTAND 3 TIER          |2200 |
|JUMBO BAG RED RETROSPOT           |2159 |
|PARTY BUNTING                     |1727 |
|LUNCH BAG RED RETROSPOT           |1638 |
|ASSORTED COLOUR BIRD ORNAMENT     |1501 |
|SET OF 3 CAKE TINS PANTRY DESIGN  |1473 |
+----------------------------------+-----+
only showing top 7 rows

# Estimated item counts of the `Description` column
>>> df.selectExpr("inline(approx_freqitems(Description))").show(7, False)
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

To pre-compute summaries for each country and estimate frequent items in some of them,
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

# Correct item counts of the `Description` column
>>> df.where("Country IN ('United Kingdom', 'Germany', 'Spain')").selectExpr("inline(approx_freqitems(Description))").show(10, False)
+----------------------------------+--------+
|item                              |estimate|
+----------------------------------+--------+
|WHITE HANGING HEART T-LIGHT HOLDER|2283    |
|JUMBO BAG RED RETROSPOT           |2042    |
|REGENCY CAKESTAND 3 TIER          |1965    |
|PARTY BUNTING                     |1647    |
|LUNCH BAG RED RETROSPOT           |1488    |
|ASSORTED COLOUR BIRD ORNAMENT     |1439    |
|SET OF 3 CAKE TINS PANTRY DESIGN  |1355    |
|LUNCH BAG  BLACK SKULL.           |1308    |
|NATURAL SLATE HEART CHALKBOARD    |1252    |
|PACK OF 72 RETROSPOT CAKE CASES   |1240    |
+----------------------------------+--------+
only showing top 10 rows

# Estimated item counts of the `Description` column
>>> df = summaries.where("Country IN ('United Kingdom', 'Germany', 'Spain')").selectExpr("approx_freqitems_combine(summaries) merged")
>>> df.selectExpr("inline(approx_freqitems_estimate(merged))").show(10, False)
+----------------------------------+---------+
|item                              |estimated|
+----------------------------------+---------+
|WHITE HANGING HEART T-LIGHT HOLDER|2292     |
|JUMBO BAG RED RETROSPOT           |2042     |
|REGENCY CAKESTAND 3 TIER          |1965     |
|PARTY BUNTING                     |1678     |
|LUNCH BAG RED RETROSPOT           |1488     |
|ASSORTED COLOUR BIRD ORNAMENT     |1442     |
|SET OF 3 CAKE TINS PANTRY DESIGN  |1437     |
|PAPER CHAIN KIT 50'S CHRISTMAS    |1310     |
|LUNCH BAG  BLACK SKULL.           |1309     |
|SPOTTY BUNTING                    |1307     |
+----------------------------------+---------+
```

### Configurations

| Property Name | Default | Meaning |
| ---- | ---- | ---- |
| spark.sql.dataSketches.freqItems.maxMapSize | 1024 | Specifies the physical size of the internal hash map managed by this sketch and must be a power of 2. The maximum capacity of this internal hash map is 0.75 times * maxMapSize. Both the ultimate accuracy and size of this sketch are functions of maxMapSize. |

## Distinct Count Sketches

Like the built-in distinct count estimation function (`approx_count_distinct`),
this plugin enalbes you to use an alternative function (`approx_count_distinct_ex`) to estimate
the distinct number of an input column in a more precise way:

```
# This example uses the BitcoinHeist data set in the UCI Machine Learning Repository:
# - https://archive.ics.uci.edu/ml/datasets/BitcoinHeistRansomwareAddressDataset
>>> df = spark.read.format("csv").option("header", True).load("BitcoinHeistData.csv").selectExpr("year", "address")
>>> df.show(5, False)
+----+----------------------------------+
|year|address                           |
+----+----------------------------------+
|2017|111K8kZAEnJg245r2cM6y9zgJGHZtJPy6 |
|2016|1123pJv8jzeFQaCV4w644pzQJzVWay2zcA|
|2016|112536im7hy6wtKbpH1qYDWtTyMRAcA2p7|
|2016|1126eDRw2wqSkWosjTCre8cjjQW8sSeWH7|
|2016|1129TSjKtx65E35GiUo4AYVeyo48twbrGX|
+----+----------------------------------+
only showing top 5 rows

>>> df.selectExpr("count(address)").show()
+--------------+
|count(address)|
+--------------+
|       2916697|
+--------------+

>>> df.selectExpr("count(distinct address)", "approx_count_distinct(address)", "approx_count_distinct_ex(address)").show()
+-----------------------+------------------------------+---------------------------------+
|count(DISTINCT address)|approx_count_distinct(address)|approx_count_distinct_ex(address)|
+-----------------------+------------------------------+---------------------------------+
|                2631095|                       2422325|                          2645708|
+-----------------------+------------------------------+---------------------------------+
```

To pre-compute summaries for each year and estimate the distinct count of addresses over specific years,
you can use similar functions to the other two sketch ones:

```
>>> import pyspark.sql.functions as f
>>> summaries = df.groupBy("year").agg(expr("approx_count_distinct_accumulate(address) AS summaries"))
>>> summaries.show()
+----+--------------------+
|year|           summaries|
+----+--------------------+
|2016|[06 01 10 0B 04 1...|
|2012|[06 01 10 0B 04 1...|
|2017|[06 01 10 0B 04 1...|
|2014|[06 01 10 0B 04 1...|
|2013|[06 01 10 0B 04 1...|
|2018|[06 01 10 0B 04 1...|
|2011|[06 01 10 0B 04 1...|
|2015|[06 01 10 0B 04 1...|
+----+--------------------+

# Correct distinct number of the `address` column
>>> df.where("year IN ('2014', '2015', '2016')").selectExpr("count(distinct address) correct").show()
+--------+
| correct|
+--------+
| 1057136|
+--------+

# Estimated distinct number of the `address` column
>>> val df = summaries.where("year IN ('2014', '2015', '2016')").selectExpr("approx_count_distinct_combine(summaries) AS merged")
>>> df.selectExpr("approx_count_distinct_estimate(merged) estimated").show()
+----------+
| estimated|
+----------+
|   1063420|
+----------+
```

### Configurations

| Property Name | Default | Meaning |
| ---- | ---- | ---- |
| spark.sql.dataSketches.distinctCnt.sketchImpl | CPC | A sketch implementation used in distinct count estimation functions. |
| spark.sql.dataSketches.distinctCnt.cpc.lgK | 11 | Specifies the parameter `lgK` for the distinct count sketch implementation named `CPC`, `CpcSketch`. |
| spark.sql.dataSketches.distinctCnt.hll.lgK | 12 | Specifies the parameter `lgK` for the distinct count sketch implementation named `HLL`, `HllSketch`. |

## TODO

 - Supports automatic function loading when initializing SparkSession (For more details, see SPARK-35380).
 - Supports the other sketch algorithms implemented in Apache DataSketches.
 - Checks performance differences between the built-in funtion and DataSketches ones.

## Bug reports

If you hit some bugs and requests, please leave some comments on [Issues](https://github.com/maropu/datasketches-spark/issues)
or Twitter([@maropu](http://twitter.com/#!/maropu)).

