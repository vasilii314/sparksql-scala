package sparkrdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import standardscala.TempData
import standardscala.TempData.toDoubleOrNeg

object RDDTempData extends {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache").setLevel(Level.WARN)
    val conf = new SparkConf()
      .setAppName("Temp Data")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("MN212142_9392.csv")

    val data = lines
      .filter(!_.contains("Day"))
      .flatMap { line =>
        val p = line.split(",")
        if (p(7) == "." || p(8) == "." || p(9) == ".") Seq.empty else
          Seq(TempData(p(0).toInt, p(1).toInt, p(2).toInt, p(4).toInt,
            toDoubleOrNeg(p(5)), toDoubleOrNeg(p(6)), p(7).toDouble, p(8).toDouble,
            p(9).toDouble))
      }.cache()

    println(data.max()(Ordering.by(_.tmax)))
    println(data.reduce((td1, td2) => if (td1.tmax >= td2.tmax) td1 else td2))

    val rainyCount = data
      .filter(_.precip >= 1.0)
      .count()
    println(s"There are $rainyCount rainy days. There is ${rainyCount * 100.0 / data.count()} percent.")

    val (rainySum, rainyCount2) = data.aggregate(0.0 -> 0)({ case ((sum, cnt), td) =>
      if (td.precip < 1.0) (sum, cnt) else (sum + td.tmax, cnt + 1)
    }, { case ((s1, c1), (s2, c2)) =>
      (s1 + s2, c1 + c2)
    })
    println(s"Average Rainy temp is ${rainySum / rainyCount2}")

    val rainyTemps = data.flatMap(td => if (td.precip < 1.0) Seq.empty else Seq(td.tmax))
    println(s"Average Rainy temp is ${rainyTemps.sum / rainyTemps.count}")

    val monthGroups = data.groupBy(_.month)
    val monthlyTemp = monthGroups.map { case (m, days) =>
      m -> days.foldLeft(0.0)((sum, td) => sum + td.tmax) / days.size
    }
    monthlyTemp.collect.sortBy(_._1) foreach println

    println(s"Stdev of highs ${data.map(_.tmax).stdev()}")
    println(s"Stdev of lows ${data.map(_.tmin).stdev()}")
    println(s"Stdev of averages ${data.map(_.tave).stdev()}")

    val keyedByYear = data.map(td => td.year -> td)
    val averageTempsByYear = keyedByYear.aggregateByKey(0.0 -> 0)({case ((sum, count), td) =>
      (sum + td.tmax, count + 1)
    }, {case ((sum, count), (sum2, count2)) => (sum + sum2, count + count2)})

    averageTempsByYear.foreach(println)

    val bins = (-20.0 to 107.0 by 1).toArray
    val counts = data.map(_.tmax).histogram(bins, evenBuckets = true)
  }
}
