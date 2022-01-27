package sparksql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.trim
import org.apache.spark.sql.{Encoders, SparkSession}


case class LAData(id: String, year: Int, period: String, value: Double)

case class Series(sid: String, area: String, measure: String, title: String)

object BLSTyped {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache").setLevel(Level.WARN)
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("BLS Typed")
      .getOrCreate()
    import spark.implicits._

    val countyData = spark.read
      .schema(Encoders.product[LAData].schema)
      .option("header", true)
      .option("delimiter", "\t")
      .csv("data/la.data.64.County")
      .select(trim($"id") as "id", $"year", $"period", $"value")
      .as[LAData]
      .sample(false, 0.1)
      .cache()
//    countyData.show()

    val series = spark.read
      .textFile("data/la.series")
      .filter(!_.contains("area_code"))
      .map { line =>
        val p = line.split("\t").map(_.trim)
        Series(p(0), p(2), p(3), p(6))
      }.cache()
//    series.show()

    val joined1 = countyData.joinWith(series, 'id === 'sid)
    joined1.show()

    spark.stop()
  }
}
