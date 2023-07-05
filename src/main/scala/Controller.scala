//package org.mt.assignments
//
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.catalyst.expressions.DateAdd
//import org.apache.spark.sql.functions._
//
//object Controller extends App {
//
//  val spark = SparkSession.builder.appName("store").master("local").getOrCreate()
//
//  val sc = spark.sparkContext
//
//  sc.setLogLevel("ERROR")
//
//  val data = spark.read.option("header", "true").csv("C:\\Users\\krishnan\\Documents\\ASSETJOURNEY_20210210_122159.csv")
//
//  data.show(false)
//
//  data.withColumn("lol",List(col("a"),col("d")))
//
//  print(data.count())
//
//  data.printSchema()
//
//  println(data.select("ASSETID").distinct().count())
//
//  var data1 = data.withColumn("FROMTIME", to_date(to_timestamp(col("FROMTIME"))))
//    .withColumn("AJTOTIME", to_date(to_timestamp(col("AJTOTIME"))))
//
//  data1 = data1.drop("ASMSTATEID", "TIMELEVEL", "VERSIONID")
//
//  data1.show()
//  data1.printSchema()
//
//
//  val diffDaysDF = data1.withColumn("diffDays", datediff(col("AJTOTIME"), col("FROMTIME")))
//
//  diffDaysDF.filter(col("diffDays") === 0).show(false)
//
//
//  def iteratoe() = udf((mon: Int) => {
//     0 until  mon toList
//  })
//
//  val exd = diffDaysDF.withColumn("listdiff", iteratoe()(col("diffDays")))
//  .withColumn("seqnum",explode(col("listdiff")))
//    .withColumn("txnDt",expr("date_add(FROMTIME, seqnum)"))
//
//    exd.show(200,false)
//
//  diffDaysDF.filter(col("diffDays") === 0).withColumn("listdiff", iteratoe()(col("diffDays")))
//    .withColumn("seqnum",explode(col("listdiff")))
//    .withColumn("txnDt",expr("date_add(FROMTIME, seqnum)")).show(false)
//
//  val edx4 = exd.drop("listdiff","diffDays","seqnum")
//
//  edx4.show(200,false)
//
//  val finalone = edx4.groupBy("AJFROMLOCID","txnDt").agg(count("ASSETID").as("INVENTORYCOUNT"))
//
//  val finalone1 = finalone.withColumnRenamed("AJFROMLOCID","LOCID").withColumnRenamed("txnDt","DATEVAL")
//  finalone1.show(false)
//
//  val finalone2= finalone1.orderBy("LOCID","DATEVAL")
//  finalone2.show(false)
//
//  print(finalone2.count())
//
//
//
//
//}