package net.jgp.books.spark.ch02.lab100_csv_to_db

import org.apache.spark.sql.functions.{lit, concat, col}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

import java.util.Properties

object CsvToRelationalDatabaseApp {
  def main(args: Array[String]) {

    val spark = SparkSession.builder.
      appName("CSV to DB").
      getOrCreate()

    val df = spark.read.
      format("csv").
      option("header", "true").
      load("data/authors.csv")

    // to use $"property" syntax, impoirt implicits
    //import spark.implicits._
    val dfWithName = df.withColumn(
      "name",
      concat(col("lname"), lit(", "), col("fname"))
      
      // like this syntax
      //concat($"lname", lit(", "), $"fname")
    )

    // print result
    dfWithName.show(5, false)

    // ref: https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html
    val connectionProperties = new Properties()
    connectionProperties.put("user", "root")
    connectionProperties.put("password", "NpzQhB3r")
    connectionProperties.put("driver", "com.mysql.cj.jdbc.Driver")

    /** 
     * Loading class `com.mysql.jdbc.Driver'. 
     * This is deprecated. The new driver class is `com.mysql.cj.jdbc.Driver'. 
     * The driver is automatically registered via the SPI and manual loading of the driver class is generally unnecessary.
    */
    //connectionProperties.put("driver", "com.mysql.jdbc.Driver")

    dfWithName.write.
      mode(SaveMode.Overwrite).
      jdbc("jdbc:mysql://localhost:3306/spark_labs", "ch02", connectionProperties)
  }
}