# Purpose
pure scala version of https://github.com/jgperrin/net.jgp.books.spark.ch02

# Environment
- Java 11
- Scala 2.13.8
- Spark 3.2.1
- Mysql 5.7.16 MySQL Community Server (GPL)
- Download mysql driver, like `mysql-connector-java-8.0.29.jar`. Save at `/replace-this-with-your-real-path`

# Database
```
CREATE DATABASE spark_labs character set utf8mb4;
```

# How to run
## 1, sbt package, in project root dir
When success, there a jar file at ./target/scala-2.13. The name is `main-scala-ch2_2.13-1.0.jar` (the same as name property in sbt file)

## 2, submit jar file, in project root dir
```
$ YOUR_SPARK_HOME/bin/spark-submit \
  --class net.jgp.books.spark.ch02.lab100_csv_to_db.CsvToRelationalDatabaseApp \
  --jars /replace-this-with-your-real-path/mysql-connector-java-8.0.29.jar
  --master local[4] \
  target/scala-2.13/main-scala-ch2_2.13-1.0.jar
```

## 3, print
```
+--------+--------------+------------------------+
|lname   |fname         |name                    |
+--------+--------------+------------------------+
|Pascal  |Blaise        |Pascal, Blaise          |
|Voltaire|François      |Voltaire, François      |
|Perrin  |Jean-Georges  |Perrin, Jean-Georges    |
|Maréchal|Pierre Sylvain|Maréchal, Pierre Sylvain|
|Karau   |Holden        |Karau, Holden           |
+--------+--------------+------------------------+
```

## 4, One more thing
```
    // to use $"property" syntax, impoirt implicits
    import spark.implicits._
    val dfWithName = df.withColumn(
      "name",
      concat($"lname", lit(", "), $"fname")
    )
```

