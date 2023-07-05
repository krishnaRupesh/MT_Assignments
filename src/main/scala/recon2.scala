//package org.mt.assignments
//
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.expressions.Window
//import org.apache.spark.sql.functions._
//import java.util.NoSuchElementException
//
//object recon2 extends App {
//
//  val spark = SparkSession.builder.appName("recon").master("local").getOrCreate()
//  val sc = spark.sparkContext
//  sc.setLogLevel("ERROR")
//
//  val scandata = spark.read.option("header", "true").csv("C:\\Users\\krishnan\\Downloads\\plantrecon_20210406_160052.csv")
//
//  scandata.show(false)
//  scandata.drop()
//  scandata.select()
//  scandata
//
//  val columns = scandata.columns
//  val new_columns = columns.map(x=> "scan_" + x )
//  val columnsRenamed = columns.zip(new_columns)
//  val result = columnsRenamed.foldLeft(scandata){(acc, names ) =>
//    acc.withColumnRenamed(names._1, names._2)
//  }
//  result.show(false)
//
//  result.withColumn("j",sum).wi
//  result.drop(new_columns:_*)
//
////  val b = scandata.withColumn("RECONTYPE", when( !(col("AUDITBATCHSTATE") <=> col("SYSTEMBATCHSTATE")), lit("Unmatched Asset Batch State")).otherwise(col("RECONTYPE")) )
////
////  b.select("RECONTYPE","AUDITBATCHSTATE","SYSTEMBATCHSTATE").show(1000,false)
////
////  b.filter(col("RECONTYPE").isNotNull)
//
//
////  val a  = scandata.filter((col("ASSETTYPE") <=> "000000000000001661"))
////
////  println(a.count())
////
////  a.show(1000,false)
////
//
//
//// val  value = scandata.filter(!col("ENTITYSTATUS").isNull).select("BARCODE").head(1)
////
////  println(value.toString)
////
////  var v:String = null
////  if (value.length != 0){
////    v = value(0).getString(0)
////  }
////
////  println(value)
////  println(v)
////  import org.apache.spark.sql.types.StringType
////  val bada = scandata.withColumn("dataset",lit(v).cast(StringType))
////  bada.write.csv("C:\\Users\\krishnan\\IdeaProjects\\MT_Assignments\\src\\main\\resources\\test")
//
////  var value =
////  try{
////
////  }catch {
////    case x: NoSuchElementException =>
////      {
////        println("in exception")
////      }
////  }
////  println(value)
//
//}
//  //including last transaction data
////  val windowbarcode = Window.partitionBy("BARCODE").orderBy(desc("SCANTIME"))
////
////  val lasttransaction = scandata.withColumn("lead_FORMTYPE", lead("FORMTYPE", 1) over windowbarcode)
////    .withColumn("lead_ORDERNUMBER",lead("ORDERNUMBER",1) over windowbarcode)
////    .withColumn("lead_SCANTIME",lead("SCANTIME",1) over windowbarcode)
////    .withColumn("lead_SCANTTYPEID",lead("SCANTTYPEID",1) over windowbarcode)
////    .withColumn("lead_BATCHSTATE",lead("BATCHSTATE",1) over windowbarcode)
////    .withColumn("lead_LINEITEMID",lead("LINEITEMID",1) over windowbarcode)
////
////  var auditdata = lasttransaction.filter(col("FORMTYPE") === "PHYSICALINVENTORY").withColumn("SCANTIME", to_timestamp(col("SCANTIME")))
////
////  var txnType = spark.read.option("header","true").csv("C:\\Users\\krishnan\\Downloads\\impalleconeu\\TRANSACTIONTYPE_20210225_163354.csv")
////
////  txnType = txnType.select("TXNTYPEID","TXNTYPEDESC")
////
////  auditdata = auditdata.join(txnType,txnType("TXNTYPEID") === auditdata("lead_SCANTTYPEID"),"left_outer")
////    .withColumnRenamed("TXNTYPEDESC","LASTTRANSACTIONACTIVITY")
////
////
////  //batch state
////  var deliveryitem = spark.read.option("header","true").csv("C:\\Users\\krishnan\\Downloads\\impalleconeu\\deliveryitem_20210226_035844.csv")
////
////  deliveryitem = deliveryitem.select("BATCHSTATE","DELIVERYITEMID","CONTAINERTYPECODE","DELIVERYNUMBER")
////    .withColumnRenamed("BATCHSTATE","new_lead_BATCHSTATE")
////
////  val batchStateNAINBOUND = auditdata.filter(col("lead_BATCHSTATE") === "NA" && col("lead_FORMTYPE") === "INBOUND")
////  val batchStateNAOUTBOUND = auditdata.filter(col("lead_BATCHSTATE") === "NA" && col("lead_FORMTYPE") === "OUTBOUND")
////
////  val batchStateNotNA = auditdata.filter(col("lead_BATCHSTATE") =!= "NA")
////
////  val batchStateNAINBOUNDjoined =  batchStateNAINBOUND.join(deliveryitem, deliveryitem("DELIVERYNUMBER") === batchStateNAINBOUND("lead_ORDERNUMBER")
////    && deliveryitem("CONTAINERTYPECODE") === batchStateNAINBOUND("ASSETTYPE") ,"left_outer").drop("DELIVERYNUMBER","DELIVERYITEMID","CONTAINERTYPECODE","lead_BATCHSTATE")
////
////  val batchStateNAOUTBOUNDjoined =  batchStateNAINBOUND.join(deliveryitem, deliveryitem("DELIVERYNUMBER") === batchStateNAINBOUND("lead_ORDERNUMBER")
////    && deliveryitem("DELIVERYITEMID") === batchStateNAINBOUND("lead_LINEITEMID") ,"left_outer").drop("DELIVERYNUMBER","DELIVERYITEMID","CONTAINERTYPECODE","lead_BATCHSTATE")
////
////  val batchStateNAjoined = batchStateNAINBOUNDjoined.union(batchStateNAOUTBOUNDjoined).withColumnRenamed("new_lead_BATCHSTATE","lead_BATCHSTATE")
////
////  val batchState =  batchStateNAjoined.union(batchStateNotNA)
////
////  batchState.show(false)
////
////  batchState.select("").show(false)
////
////
////
////
////
////}
