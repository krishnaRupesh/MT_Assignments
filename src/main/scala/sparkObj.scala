import org.apache.spark._
import sys.process._

object sparkObj {

  def main(args:Array[String]):Unit={

    val conf = new SparkConf().setAppName("First").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")


    val data = sc.textFile("file:///home/cloudera/data/txns")
    val gymdata = data.filter(x=>x.contains("Gymnastics"))

    "hadoop fs -rmr /user/cloudera/gymdata".!

    gymdata.saveAsTextFile("/user/cloudera/gymdata")


  }
}
