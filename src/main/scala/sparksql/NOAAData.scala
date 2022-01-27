package sparksql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, StringType, StructField, StructType}


object NOAAData {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache").setLevel(Level.WARN)
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("NOAA Data")
      .getOrCreate()
    import spark.implicits._

    val tschema = StructType(Array(
      StructField("sid", StringType),
      StructField("date", DateType),
      StructField("mtype", StringType),
      StructField("value", DoubleType)
    ))

    val data2017 = spark.read.schema(tschema).option("dateFormat", "yyyyMMdd").csv("data/2017.csv")
//    data2017.show()
//    data2017.schema.printTreeString()

    val sschema = StructType(Array(
      StructField("sid", StringType),
      StructField("lat", DoubleType),
      StructField("lon", DoubleType),
      StructField("name", StringType)
    ))

    val stationRDD = spark.sparkContext.textFile("data/ghcnd-stations.txt")
      .map { line =>
        val id = line.substring(0, 11)
        val lat = line.substring(12, 20).toDouble
        val lon = line.substring(21, 30).toDouble
        val name = line.substring(41, 71)
        Row(id, lat, lon, name)
      }

    val stations = spark.createDataFrame(stationRDD, sschema).cache()

    data2017.createOrReplaceTempView("data2017")

    val pureSQL = spark.sql(
      """
        |SELECT sid, date, (((tmin + tmax) / 2) * 1.8 + 32) AS tave
        |FROM
        |(SELECT sid, date, value AS tmax
        |FROM data2017
        |WHERE mtype = "TMAX"
        |LIMIT 1000)
        |JOIN
        |(SELECT sid, date, value AS tmin
        |FROM data2017
        |WHERE mtype = "TMIN"
        |LIMIT 1000)
        |USING (sid, date)
        |""".stripMargin)

    pureSQL.show()

//    val tmax2017 = data2017.where($"mtype" === "TMAX").limit(100).drop("mtype").withColumnRenamed("value", "tmax")
////    tmax2017.describe().show()
//
//    val tmin2017 = data2017.where($"mtype" === "TMIN").limit(100).drop("mtype").withColumnRenamed("value", "tmin")
////    tmin2017.describe().show()
//
//    val combinedTemps2017 = tmax2017.join(tmin2017, Seq("sid", "date"))
////    combinedTemps2017.show()
//
//    val averageTemp2017 = combinedTemps2017.select($"sid", $"date", ($"tmax" + $"tmin") / 2).withColumnRenamed("((tmax + tmin) / 2)", "tave")
////    averageTemp2017.show()
//
//    val stationTemps2017 = averageTemp2017.groupBy($"sid").agg(avg($"tave"))
////    stationTemps2017.show()
////    stations.show()
//
//    val joinedData = stationTemps2017.join(stations, "sid")
////    joinedData.show()
//
//
////    averageTemp2017.show()
    spark.stop()
  }
}
