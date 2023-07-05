package org.mt.assignments

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
//from delta.tables
import io.delta.tables._

import java.util.{Date, UUID}
import org.apache.hadoop.fs.{FileSystem, Path}

object IM1059 extends App {

  val spark = SparkSession.builder.appName("recon").master("local").getOrCreate()
  val sc = spark.sparkContext
  sc.setLogLevel("ERROR")

  val SCANDATA_df = spark.read.option("header", "true").csv("C:\\Users\\krishnan\\Downloads\\stg2\\scandata_20210430_041312.csv")
  val ASSET_df = spark.read.option("header", "true").csv("C:\\Users\\krishnan\\Downloads\\stg2\\asset_20210430_041646.csv")
  val DELIVERY_df = spark.read.option("header", "true").csv("C:\\Users\\krishnan\\Downloads\\stg2\\delivery_20210430_041536.csv")
  val DELIVERYITEMS_df = spark.read.option("header", "true").csv("C:\\Users\\krishnan\\Downloads\\stg2\\deliveryitems_20210430_041506.csv")
  val LOCATION_df = spark.read.option("header", "true").csv("C:\\Users\\krishnan\\Downloads\\stg2\\location_20210511_134606.csv")
  val ASSETTYPE_df = spark.read.option("header", "true").csv("C:\\Users\\krishnan\\Downloads\\stg2\\ASSETTYPE_20210511_134946.csv")
  val PLANTINTERNALMOVEMENTS_df = spark.read.option("header", "true").csv("C:\\Users\\krishnan\\Downloads\\stg2\\plantmovement_20210511_135206.csv")




  def numPertition ( loc:String) :Int = {

    val fs: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    (fs.getContentSummary(new Path(loc)).getLength/134217728).ceil.toInt
    fs.listLocatedStatus(new Path(loc))
  }

  SCANDATA_df.repartition()







  import spark.implicits._

  val generateUUID = udf(() => UUID.randomUUID().toString)
  val df1 = Seq(("id1", 1), ("id2", 4), ("id3", 5)).toDF("id", "value")
  val df2 = df1.withColumn("UUID", generateUUID())
  val dsfs = df1.withColumn("fs",monotonically_increasing_id())


//  val scandata = SCANDATA_df.filter(!(col("ISDELETED") <=> true)).filter(col("SCANTTYPEID").isNotNull).select("BARCODE", "SCANTIME", "SCANLOCID", "SCANDATE", "BARCODESTATUS", "BATCHSTATE", "FORMTYPE", "LINEITEMID", "ORDERNUMBER").filter(col("FORMTYPE").isin("PLANTACTIVITY", "INBOUND", "OUTBOUND")).withColumnRenamed("SCANLOCID", "LOCID")
//  val assets = ASSET_df.select("ASSETID", "ASSETTYPE")
//  val assettype = ASSETTYPE_df.select("ASSETTYPE", "ASSETTYPENAME")
//  val delivery = DELIVERY_df.select("DELIVERYNUMBER", "ORDERTYPE")
//  val deliveryitems = DELIVERYITEMS_df.filter(!(col("ISDELETED") <=> true)).select("DELIVERYITEMID", "DELIVERYNUMBER", "CONTAINERTYPECODE", "BATCHSTATE").withColumnRenamed("BATCHSTATE", "delivery_BATCHSTATE")
//  val location = LOCATION_df.select("LOCID", "LOCNAME")
//  val plantdata = PLANTINTERNALMOVEMENTS_df.select("ASSETTYPENAME", "ASSETTYPE", "SCANQTY", "DOCUMENTDATE", "SCANDATE", "LOCNAME", "LOCID", "MOVEMENTID", "UOM", "TOBATCHSTATE", "FROMBATCHSTATE", "ISCORRECTED", "ISPROCESSED")
//
//  val data = scandata.join(assets, scandata("BARCODE") === assets("ASSETID")).drop("BARCODE")
//  val deliveryinfo = deliveryitems.join(delivery, deliveryitems("DELIVERYNUMBER") === delivery("DELIVERYNUMBER")).drop(delivery("DELIVERYNUMBER")).withColumnRenamed("BATCHSTATE", "delivery_BATCHSTATE")
//  val data1 = data.filter(col("FORMTYPE") === "OUTBOUND").join(deliveryinfo.filter(col("ORDERTYPE") === 1), deliveryinfo("DELIVERYNUMBER") === data("ORDERNUMBER") && deliveryinfo("DELIVERYITEMID") === data("LINEITEMID")).select("SCANTIME", "LOCID", "SCANDATE", "BARCODESTATUS", "FORMTYPE", "LINEITEMID", "ORDERNUMBER", "ASSETID", "ASSETTYPE", "delivery_BATCHSTATE")
//  val data2 = data.filter(col("FORMTYPE") === "INBOUND").join(deliveryinfo.filter(col("ORDERTYPE") === 2), deliveryinfo("DELIVERYNUMBER") === data("ORDERNUMBER") && deliveryinfo("CONTAINERTYPECODE") === data("ASSETTYPE")).select("SCANTIME", "LOCID", "SCANDATE", "BARCODESTATUS", "FORMTYPE", "LINEITEMID", "ORDERNUMBER", "ASSETID", "ASSETTYPE", "delivery_BATCHSTATE")
//  val data3 = data.filter(col("FORMTYPE").isin("OUTBOUND", "INBOUND")).join(deliveryinfo.filter(col("ORDERTYPE") === 3), deliveryinfo("DELIVERYNUMBER") === data("ORDERNUMBER"))
//  val data4 = data3.filter(col("FORMTYPE") === "OUTBOUND" && col("LINEITEMID") === col("DELIVERYITEMID"))
//  val data5 = data3.join(data4, data3("ASSETID") === data4("ASSETID") && data3("DELIVERYITEMID") === data4("DELIVERYITEMID") && data3("DELIVERYNUMBER") === data4("DELIVERYNUMBER"), "left_semi").select("SCANTIME", "LOCID", "SCANDATE", "BARCODESTATUS", "FORMTYPE", "LINEITEMID", "ORDERNUMBER", "ASSETID", "ASSETTYPE", "delivery_BATCHSTATE")
//  val batchstate = data1.union(data2).union(data5).withColumnRenamed("delivery_BATCHSTATE", "BATCHSTATE").union(data.filter(col("FORMTYPE") === "PLANTACTIVITY").select("SCANTIME", "LOCID", "SCANDATE", "BARCODESTATUS", "FORMTYPE", "LINEITEMID", "ORDERNUMBER", "ASSETID", "ASSETTYPE", "BATCHSTATE"))
//
//  import org.apache.spark.sql.expressions.Window
//
//  val windowbatch = Window.partitionBy("ASSETID", "LOCID").orderBy("SCANTIME")
//  val newdata = batchstate.withColumn("rank", rank over windowbatch).withColumn("FROMBATCHSTATE", lag("BATCHSTATE", 1) over windowbatch)
//  val filterdata = newdata.filter(col("BATCHSTATE") =!= col("FROMBATCHSTATE"))
//  val groupeddata = filterdata.groupBy("LOCID", "SCANDATE", "ASSETTYPE", "FROMBATCHSTATE", "BATCHSTATE").agg(count("*").as("SCANQTY"))
//
//
//  val locname = groupeddata.join(location, "LOCID").join(assettype, "ASSETTYPE")
//  val allrows = locname.withColumn("MOVEMENTID", concat(col("ASSETTYPE"), col("LOCID"), col("SCANDATE"), col("FROMBATCHSTATE"), col("BATCHSTATE"))).withColumn("DOCUMENTDATE", current_date).withColumn("UOM", lit("EA")).withColumnRenamed("BATCHSTATE", "TOBATCHSTATE").select("ASSETTYPENAME", "ASSETTYPE", "SCANQTY", "DOCUMENTDATE", "SCANDATE", "LOCNAME", "LOCID", "MOVEMENTID", "UOM", "TOBATCHSTATE", "FROMBATCHSTATE")
//  val newrows = allrows.join(plantdata.filter(col("ISPROCESSED") === true), allrows("MOVEMENTID") === plantdata("MOVEMENTID") && allrows("SCANQTY") === plantdata("SCANQTY"), "left_anti").withColumn("ISCORRECTED", lit(false)).withColumn("ISPROCESSED", lit(false))
//  val updatedrows = allrows.join(plantdata.filter(col("ISPROCESSED") === true), allrows("MOVEMENTID") === plantdata("MOVEMENTID") && allrows("SCANQTY") =!= plantdata("SCANQTY"), "left_semi").withColumn("ISCORRECTED", lit(true)).withColumn("ISPROCESSED", lit(false))
//  val zerovalues_unprocessed = plantdata.filter(col("ISPROCESSED") === false).join(newrows, newrows("MOVEMENTID") === plantdata("MOVEMENTID"), "left_anti").withColumn("SCANQTY", lit(0)).withColumn("ISCORRECTED", lit(false))
//  val zerovalues_processed = plantdata.filter(col("ISPROCESSED") === true).join(allrows, allrows("MOVEMENTID") === plantdata("MOVEMENTID"), "left_anti").withColumn("SCANQTY", lit(0)).withColumn("ISCORRECTED", lit(true)).withColumn("ISPROCESSED", lit(false))
//  val finalone = newrows.union(updatedrows).union(zerovalues_unprocessed).union(zerovalues_processed)
//
//  //finalone.explain(false)
//
//  finalone.show(false)
//
//  scandata.withColumn("TOLOCID", when(col("FORMTYPE") === "CUSTOMEROUTBOUND",col("SCANTOLOCID")).when(col("FORMTYPE") === "CUSTOMERINBOUND", col("SCANLOCID")))
//
//
//  scandata.withColumn("FROMLOCID", when(col("FORMTYPE") === "CUSTOMEROUTBOUND",col("SCANLOCID")).when(col("FORMTYPE") === "CUSTOMERINBOUND", col("SCANTOLOCID")))
//
//
//
//  scandata.explain(extended = true)
//
//  scandata.withColumn("da",col("ds")+col("sds"))



}