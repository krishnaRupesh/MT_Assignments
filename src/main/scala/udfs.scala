//package org.mt.assignments
//
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.expressions.UserDefinedFunction
//import org.apache.spark.sql.functions._
//
//object udfs extends App {
//
//  //scandata_stg2_20210422_102302.csv
//
//  val spark = SparkSession.builder.appName("store").master("local").getOrCreate()
//
//  val sc = spark.sparkContext
//
//  sc.setLogLevel("ERROR")
//
//  val data = spark.read.option("header", "true").csv("C:\\Users\\krishnan\\Downloads\\scandata_stg2_20210422_102302.csv")
//
//  val scandata =  data.select("BARCODE","SCANTIME","SCANLOCID","SCANDATE","BARCODESTATUS","BATCHSTATE","FORMTYPE").filter(col("FORMTYPE").isin("OUTBOUND","INBOUND","PLANTACTIVITY"))
//
//  import org.apache.spark.sql.expressions.Window
//
//
//  val windowbatch = Window.partitionBy("BARCODE","SCANLOCID").orderBy(desc("SCANTIME"))
//
//
//
//  val scandata0 = scandata.withColumn("lead_formtype", lead("FORMTYPE",1) over windowbatch).withColumn("rank", rank over windowbatch)
//
//  val scandata1 = scandata0.groupBy("BARCODE","SCANLOCID").agg(collect_list("FORMTYPE"),collect_list("lead_formtype"),collect_list("rank"))
//
//  scandata1.show(10000,false)
//
//
//
//  def dateChange() = udf ((formlist: Seq[String]) =>{
//
//    if(formlist.size > 2 ) {
//      for(i <- formlist) {
//        if (i == "")
//      }
//
//    }
//
//  })
//}
