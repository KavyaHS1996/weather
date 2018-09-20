import java.util.regex.Pattern

import org.apache.spark.sql.SparkSession

object Maxyear {

  def getmax(temp: Array[String]):String={
    var maxtemp=0d
    val tempr=temp(0)
    if(tempr.toString.toDouble>maxtemp)
      {
        maxtemp=tempr.toDouble
      }
    return maxtemp.toString
  }

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("weather").master("local").
      getOrCreate()
    val sc = sparkSession.sparkContext
    val rdd = sc.textFile("c:\\datasets\\weather\\weather_2009_2016.csv")
    rdd.map(line=>line.split(",")).map(line=>(patten(line(0)),line(3))).groupByKey().map(data=>((data._1,getmax(data._2.toArray)))).foreach(println)
  }

  def patten(string: String): String = {
    var pattern = Pattern.compile("\\d+.\\d+.(\\d+).*")
    var line = string
    var split=line.split(",")(0)
    var matches = pattern.matcher(split)
    if (matches.find()) {
      return matches.group(1)
    }
    return null
  }
}




