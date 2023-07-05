package org.mt.assignments

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object test extends App{

  val spark = SparkSession.builder.appName("recon").master("local").getOrCreate()
  val sc = spark.sparkContext
  sc.setLogLevel("ERROR")
  import spark.implicits._
  val i1 = sc.parallelize(Seq(("a", "string"), ("another", "string"), ("last", "one"))).toDF("a", "b")
  val i2 = Seq(("one", "string"), ("two", "strings")).toDF("a", "b")
  val i1Idx = i1.withColumn("sourceId", lit(1))
  val i2Idx = i2.withColumn("sourceId", lit(2))
  val input = i1Idx.union(i2Idx)
  input.show(false)
  val weights = Seq((1, 0.6), (2, 0.4)).toDF("sourceId", "weight")
  weights.show(false)
  weights.join(input, "sourceId").show
}
