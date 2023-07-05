package org.mt.assignments

import org.apache.spark.sql.SparkSession
import scala.collection.mutable.ListBuffer
import org.joda.time.{DateTime, DateTimeZone, Days}
import org.joda.time.format.DateTimeFormat
import org.apache.spark.sql.functions._

object test2 extends App {
  val spark = SparkSession.builder.appName("recon").master("local").getOrCreate()
  val sc = spark.sparkContext
  sc.setLogLevel("ERROR")

  val SCANDATA_df = spark.read.option("header", "true").csv("C:\\Users\\krishnan\\Downloads\\stg2\\scandata_20210430_041312.csv")

  val p = SCANDATA_df.select("ASSETTYPE","ISDELETED").distinct().groupBy("ASSETTYPE").agg(collect_list("ISDELETED").as("BOOLLIST"))
  SCANDATA_df.printSchema()
  val p2 = p.withColumn("test",when(col("BOOLLIST").contains(Set("true")) , lit("there")).otherwise(lit("notthere")))
  p.show(false)
  p.printSchema()
  p2.show(false)

  p2.withColumn("lol", last() over )
//  case class Asset(assetid: String, status: String, validfrom: String, validfromdate: String, reason: String, oldbarcode: String)
//
//  case class AssetTransition(assetid: String, stateid: String, tostateid: String, validfromtime: String, validfromdate: String, validto: Option[String], validtodate: Option[String], reason: String, toreason: String, userid: String, oldbarcode: String)
//
//  case class ScanData(barcode: String, barcodestatus: String, scantime: String, scantimeInMillis: Long, createdat: String, createdatInMillis: Long, reason: String, userid: String, oldbarcode: String)
//
//  case class Result(assetid: String, asset: Asset, assetTransitions: List[AssetTransition])
//
//    val scandf = SCANDATA_df.filter("coalesce(FORMTYPE,'FORM') != 'PHYSICALINVENTORY' ")
//    // val scandata1 = scandf.filter($"barcode" === "8003006625100166831000000058")
//    val scanFullDf1 = scandf.select($"barcode", when(($"barcodestatus".isNull or $"barcodestatus" === ""), lit("IN USE")).otherwise($"barcodestatus").alias("barcodestatus"), $"scantime", $"isdeleted", $"createdat", when(($"batchsubstate".isNull or $"batchsubstate" === ""), lit("null")).otherwise($"batchsubstate").alias("reason"), when(($"scanttypeid".isNull or $"scanttypeid" === ""), lit("null")).otherwise($"scanttypeid").alias("scanttypeid"), when(($"createdby".isNull or $"createdby" === ""), lit("null")).otherwise($"createdby").alias("createdby"), when(($"scanuser".isNull or $"scanuser" === ""), lit("null")).otherwise($"scanuser").alias("scanuser"), when(($"oldbarcode".isNull or $"oldbarcode" === ""), lit("null")).otherwise($"oldbarcode").alias("oldbarcode"))
//
//    val solutionDF = scanFullDf1.where($"isdeleted" =!= true).groupBy($"barcode").agg(concat_ws(",", collect_list($"barcodestatus")).alias("barcodestatus"), concat_ws(",", collect_list($"scantime")).alias("scantime"), concat_ws(",", collect_list($"createdat")).alias("createdat"), concat_ws(",", collect_list($"reason")).alias("reason"), concat_ws(",", collect_list($"scanttypeid")).alias("scanttypeid"), concat_ws(",", collect_list($"createdby")).alias("createdby"), concat_ws(",", collect_list($"scanuser")).alias("scanuser"), concat_ws(",", collect_list($"oldbarcode")).alias("oldbarcode"))
//      .select($"barcode", $"barcodestatus", $"scantime", $"createdat", $"reason", $"scanttypeid", $"createdby", $"scanuser", $"oldbarcode").map {
//      row =>
//        val barcode = row.getString(0)
//        val barcodestatus = row.getString(1).split(",")
//        val scantime = row.getString(2).split(",")
//        val createdat = row.getString(3).split(",")
//        val reason = row.getString(4).split(",")
//        val scanttypeid = row.getString(5).split(",")
//        val createdby = row.getString(6).split(",")
//        val scanuser = row.getString(7).split(",")
//        val oldbarcode = row.getString(8).split(",")
//
//        val scanDataList = new ListBuffer[ScanData]()
//        val usersList = new ListBuffer[String]()
//        var userid = ""
//        for (i <- 0 until barcodestatus.length) {
//          if (Option(scanttypeid(i)).getOrElse("") == "null" || Option(scanttypeid(i)).getOrElse("").isEmpty) {
//            if (!Option(createdby(i)).getOrElse("").isEmpty)
//              userid = createdby(i)
//          }
//          else {
//            if (!Option(scanuser(i)).getOrElse("").isEmpty)
//              userid = scanuser(i)
//          }
//          scanDataList += ScanData(barcode, barcodestatus(i), scantime(i), DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").parseDateTime(scantime(i)).getMillis, createdat(i), DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").parseDateTime(createdat(i)).getMillis, reason(i), userid, oldbarcode(i))
//        }
//
//        val scanDataOrderList = scanDataList.sortBy(r => (r.scantimeInMillis, r.createdatInMillis)).toList
//
//        var assetState = scanDataOrderList(0).barcodestatus
//        var assetStateReason = scanDataOrderList(0).reason
//        var oldassetid = ""
//        // var assetState = scanDataOrderList(0).barcodestatus
//
//        val assetTransitionList = new ListBuffer[AssetTransition]()
//        assetTransitionList += AssetTransition(scanDataOrderList(0).barcode, scanDataOrderList(0).barcodestatus, "", scanDataOrderList(0).scantime, DateTimeFormat.forPattern("yyyy-MM-dd").print(DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").parseDateTime(scanDataOrderList(0).scantime)), None, None, scanDataOrderList(0).reason, "", scanDataOrderList(0).userid, scanDataOrderList(0).oldbarcode)
//
//        for (i <- 1 until scanDataOrderList.length) {
//          if (!assetState.equals(scanDataOrderList(i).barcodestatus) || !assetStateReason.equals(scanDataOrderList(i).reason)) {
//
//            val oldvalidfrom = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").parseDateTime(assetTransitionList(assetTransitionList.length - 1).validfromtime)
//            val newvalidfrom = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").parseDateTime(scanDataOrderList(i).scantime)
//
//            val oldvalidfromdate = DateTimeFormat.forPattern("yyyyMMdd").print(oldvalidfrom).toInt
//            val newvalidfromdate = DateTimeFormat.forPattern("yyyyMMdd").print(newvalidfrom).toInt
//
//            var adddays = 0
//            if (newvalidfromdate > oldvalidfromdate)
//              adddays = -1
//
//            val oldassettransition = assetTransitionList(assetTransitionList.length - 1).copy(validto = Some(DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").print(DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").parseDateTime(scanDataOrderList(i).scantime).plusDays(adddays))), validtodate = Some(DateTimeFormat.forPattern("yyyy-MM-dd").print(DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").parseDateTime(scanDataOrderList(i).scantime).plusDays(adddays))), tostateid = scanDataOrderList(i).barcodestatus, stateid = assetState, toreason = scanDataOrderList(i).reason, reason = assetStateReason)
//            val newassettransition = AssetTransition(scanDataOrderList(i).barcode, scanDataOrderList(i).barcodestatus, "", scanDataOrderList(i).scantime, DateTimeFormat.forPattern("yyyy-MM-dd").print(DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").parseDateTime(scanDataOrderList(i).scantime)), None, None, scanDataOrderList(i).reason, "", scanDataOrderList(i).userid, scanDataOrderList(i).oldbarcode)
//
//            assetTransitionList -= assetTransitionList(assetTransitionList.length - 1)
//            assetTransitionList += (oldassettransition)
//            assetTransitionList += (newassettransition)
//
//            assetState = newassettransition.stateid
//            assetStateReason = newassettransition.reason
//            if (assetState == "IN USE" && (assetStateReason == "DTR" || assetStateReason == "LTR")) {
//              oldassetid = scanDataOrderList(i).oldbarcode
//            }
//          }
//        }
//        val lastAssetTransition = assetTransitionList(assetTransitionList.length - 1)
//        // assetTransitionList.toList
//        Result(barcode, Asset(lastAssetTransition.assetid, lastAssetTransition.stateid, lastAssetTransition.validfromtime, lastAssetTransition.validfromdate, lastAssetTransition.reason, oldassetid), assetTransitionList.toList)
//    }
//
//    val p1 = scandf.withColumn("AUDITSTARTDATE",to_date(col("AUDITSTARTTIME")))
//      .select("LOCID","AUDITSTARTDATE","ISDUPLICATE").distinct()
//      .groupBy("LOCID","AUDITSTARTDATE").agg(collect_list("ISDUPLICATE").as("BOOLDUPE"))
//
//    val p2 = p1.withColumn("RECONSTATUS" , when(col("BOOLDUPE") contains true , lit("AS")).otherwise(lit("AD")))
//
//

}
