package org.mt.assignments
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object Controller2 extends App{

  val spark = SparkSession.builder.appName("store").master("local").getOrCreate()
  val sc = spark.sparkContext
  sc.setLogLevel("ERROR")

  val data = spark.read.option("header", "true").csv("C:\\Users\\krishnan\\IdeaProjects\\MT_Assignments\\src\\main\\resources\\all_rows_20210218_060632.csv")
  //data.show(false)
  // selecting required columns
  var data1 =  data.select("FROMTIME","ASMSTATEID","ASSETID","AJFROMLOCID","AJTOLOCID","AJTOTIME","AJSTATUS")
  //data1.show(false)
  //data1.printSchema()
  data1 = data1.withColumn("FROMTIME", to_timestamp(col("FROMTIME")))
    .withColumn("AJTOTIME", to_timestamp(col("AJTOTIME")))

  //data1.printSchema()

  data1.groupBy("AJSTATUS").agg(count("*").as("AJSTATUS_COUNT")).show(false)

  //removing discard
  data1 = data1.filter(col("AJSTATUS") =!= "DISCARD")

 println( data1.count())

  data1.orderBy("ASSETID","FROMTIME").show(200,false)

  //closed records
  val closed = data1.filter(col("AJSTATUS") =!= "CLOSED")
  val opened = data1.filter(col("AJSTATUS") =!= "OPEN")

//  closed.show(200,false)
//  opened.show(200,false)

  val windowassert = Window.partitionBy("ASSETID").orderBy("FROMTIME")

  val result = data1.withColumn("rank", cume_dist() over windowassert).filter(col("rank") === 1)

  result.show(false)
  println(result.count())







}
