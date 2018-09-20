import org.apache.spark.sql.SparkSession

object Weather {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("weather").master("local").
      getOrCreate()
    val sc = sparkSession.sparkContext
    val rdd = sc.textFile("c:\\datasets\\weather\\weather_2009_2016.csv")
    var temp = rdd.map(line => (line.split(",")(3)))
    println(temp.max())

  }
}











