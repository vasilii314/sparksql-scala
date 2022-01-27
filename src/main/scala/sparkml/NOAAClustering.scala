package sparkml

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.clustering
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.sql.functions.{cos, dayofyear, sin}
import org.apache.spark.sql.{Encoders, SparkSession}

import java.sql.Date

case class Station(sid: String, lat: Double, lon: Double, elev: Double, name: String)
case class NOAAData(sid: String, date: Date, measure: String, value: Double)

object NOAAClustering {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache").setLevel(Level.WARN)
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("NOAA Data")
      .getOrCreate()
    import spark.implicits._

    val stations = spark.read
      .textFile("data/ghcnd-stations.txt")
      .map { line =>
        val id = line.substring(0, 11)
        val lat = line.substring(12, 20).trim.toDouble
        val lon = line.substring(21, 30).trim.toDouble
        val elev = line.substring(31, 37).trim.toDouble
        val name = line.substring(41, 71)
        Station(id, lat, lon, elev, name)
      }.cache()

    val stationsVA = new VectorAssembler().setInputCols(Array("lat", "lon")).setOutputCol("location")
    val stationsWithLoc = stationsVA.transform(stations)

    val kMeans = new clustering.KMeans().setK(2000).setFeaturesCol("location").setPredictionCol("cluster")
    val stationClusterModel = kMeans.fit(stationsWithLoc)

    val stationsWithClusters = stationClusterModel.transform(stationsWithLoc)
    stationsWithClusters.show()

    val x = stationsWithClusters.select('lon).as[Double].collect()

    val y = stationsWithClusters.select('lat).as[Double].collect()

    val predict = stationsWithClusters.select('cluster).as[Double].collect()

    val data2017 = spark.read
      .schema(Encoders.product[NOAAData].schema)
      .option("dateFormat", "yyyyMMdd")
      .csv("data/2017.csv")

    val clusterStations = stationsWithClusters
      .filter('cluster === 441)
      .select('sid)

    val clusterData = data2017.filter('measure === "TMAX").join(clusterStations, "sid")

    val withDOYInfo = clusterData
      .withColumn("doy", dayofyear($"date"))
      .withColumn("doySin", sin('doy / 365 * 2 * math.Pi))
      .withColumn("doyCos", cos('doy / 365 * 2 * math.Pi))

    val linearRegData = new VectorAssembler()
      .setInputCols(Array("doySin", "doyCos"))
      .setOutputCol("doyTrig")
      .transform(withDOYInfo)
      .cache()

    val linearReg = new LinearRegression()
      .setFeaturesCol("doyTrig")
      .setLabelCol("value")
      .setMaxIter(10)
      .setPredictionCol("pMaxTemp")

    val linerRegModel = linearReg.fit(linearRegData)

    println(linerRegModel.coefficients + " " + linerRegModel.intercept)

    val withLinearFit = linerRegModel.transform(linearRegData)

    val doy = withLinearFit.select('doy).as[Double].collect()
    val maxTemp = withLinearFit.select('value).as[Double].collect()
    val pMaxTemp = withLinearFit.select('pMaxTemp).as[Double].collect()

    spark.stop()
  }
}
