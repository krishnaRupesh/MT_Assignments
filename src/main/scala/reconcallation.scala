//package org.mt.assignments
//
//import org.apache.spark.sql.{DataFrame, SparkSession}
//import org.apache.spark.sql.expressions.Window
//import org.apache.spark.sql.functions._
//
//object reconcallation extends App{
//  // spark Session
//  val spark = SparkSession.builder.appName("recon").master("local").getOrCreate()
//  val sc = spark.sparkContext
//  sc.setLogLevel("ERROR")
//
//  var scandata =  spark.read.option("header","true").csv("C:\\Users\\krishnan\\Downloads\\impalleconeu\\scan_data_20210223_054447.csv")
//
//  scandata = scandata.select("ASSETTYPE","BARCODE","LINEITEMID","ORDERNUMBER","SCANLOCID","SCANTIME","SCANTTYPEID","BATCHSTATE","FORMTYPE")
//    .withColumn("SCANTIME", to_timestamp(col("SCANTIME")))
//
//  val reconciled = scandata.withColumn("ISRECONCILED", when(col("RECONTYPE") === "Valid Asset" , lit(true)).otherwise(lit(false)))
//  //including last transaction data
//  val windowbarcode = Window.partitionBy("BARCODE").orderBy(desc("SCANTIME"))
//
//  val lasttransaction = scandata.withColumn("lead_FORMTYPE", lead("FORMTYPE", 1) over windowbarcode)
//    .withColumn("lead_ORDERNUMBER",lead("ORDERNUMBER",1) over windowbarcode)
//    .withColumn("lead_SCANTIME",lead("SCANTIME",1) over windowbarcode)
//    .withColumn("lead_SCANTTYPEID",lead("SCANTTYPEID",1) over windowbarcode)
//    .withColumn("lead_BATCHSTATE",lead("BATCHSTATE",1) over windowbarcode)
//    .withColumn("lead_LINEITEMID",lead("LINEITEMID",1) over windowbarcode)
//  var auditdata = lasttransaction.filter(col("FORMTYPE") === "PHYSICALINVENTORY")
//
//  //transaction type
//  var txnType = spark.read.option("header","true").csv("C:\\Users\\krishnan\\Downloads\\impalleconeu\\TRANSACTIONTYPE_20210225_163354.csv")
//  txnType = txnType.select("TXNTYPEID","TXNTYPEDESC")
//  auditdata = auditdata.join(txnType,txnType("TXNTYPEID") === auditdata("lead_SCANTTYPEID"),"left_outer")
//    .withColumnRenamed("TXNTYPEDESC","LASTTRANSACTIONACTIVITY")
//  auditdata =  auditdata.withColumn("LASTTRANSACTIONDATE",to_date(col("lead_SCANTIME")))
//    .withColumn("LASTTRANSACTIONDELNUMBER",col("lead_ORDERNUMBER"))
//
//
//
//  //batch state
//  var deliveryitem = spark.read.option("header","true").csv("C:\\Users\\krishnan\\Downloads\\impalleconeu\\deliveryitem_20210226_035844.csv")
//  deliveryitem = deliveryitem.select("BATCHSTATE","DELIVERYITEMID","CONTAINERTYPECODE","DELIVERYNUMBER")
//    .withColumnRenamed("BATCHSTATE","new_lead_BATCHSTATE")
//  val batchStateNAINBOUND = auditdata.filter(col("lead_BATCHSTATE") === "NA" && col("lead_FORMTYPE") === "INBOUND")
//  val batchStateNAOUTBOUND = auditdata.filter(col("lead_BATCHSTATE") === "NA" && col("lead_FORMTYPE").isin() "OUTBOUND")
//  val batchStateNotNA = auditdata.filter(col("lead_BATCHSTATE") =!= "NA")
//  val batchStateNotNA = auditdata.filter(col("lead_BATCHSTATE").isNull)
//  val batchStateNAINBOUNDjoined =  batchStateNAINBOUND.join(deliveryitem, deliveryitem("DELIVERYNUMBER") === batchStateNAINBOUND("lead_ORDERNUMBER")
//    && deliveryitem("CONTAINERTYPECODE") === batchStateNAINBOUND("ASSETTYPE") ,"left_outer").drop("DELIVERYNUMBER","DELIVERYITEMID","CONTAINERTYPECODE","lead_BATCHSTATE")
//  val batchStateNAOUTBOUNDjoined =  batchStateNAINBOUND.join(deliveryitem, deliveryitem("DELIVERYNUMBER") === batchStateNAINBOUND("lead_ORDERNUMBER")
//    && deliveryitem("DELIVERYITEMID") === batchStateNAINBOUND("lead_LINEITEMID") ,"left_outer").drop("DELIVERYNUMBER","DELIVERYITEMID","CONTAINERTYPECODE","lead_BATCHSTATE")
//  val batchStateNAjoined = batchStateNAINBOUNDjoined.union(batchStateNAOUTBOUNDjoined).withColumnRenamed("new_lead_BATCHSTATE","lead_BATCHSTATE")
//  val batchdata =  batchStateNotNA.union(batchStateNAjoined)
//  auditdata = batchdata.withColumnRenamed("lead_BATCHSTATE","SYSTEMBATCHSTATE")
//
//  auditdata.show(false)
//
//
//  val batchdata1 = auditdata.withColumn("SYSTEMBATCHSTATE",
//    when(col("lead_BATCHSTATE") =!= "NA",col("lead_BATCHSTATE"))
//      .when(col("lead_BATCHSTATE") ==="NA" and col("lead_FORMTYPE") ==="INBOUND" ,batchStateNAINBOUNDjoined("new_lead_BATCHSTATE")).
//      when(col("lead_BATCHSTATE") ==="NA" and col("lead_FORMTYPE") ==="OUTBOUND",batchStateNAOUTBOUNDjoined("new_lead_BATCHSTATE")))
//
//
//
//
//  //selected columns
//  auditdata = auditdata.select("BARCODE","SCANTIME","SCANLOCID","BATCHSTATE","SYSTEMBATCHSTATE",
//    "LASTTRANSACTIONDELNUMBER","LASTTRANSACTIONDATE","LASTTRANSACTIONACTIVITY")
//    .withColumnRenamed("SCANTIME","AUDITTIME")
//    .withColumnRenamed("SCANLOCID","AUDITLOCATIONID")
//    .withColumnRenamed("BATCHSTATE","AUDITBATCHSTATE")
//    .withColumnRenamed("BARCODE","ASSETID")
//
//
//  var locdata = spark.read.option("header","true").csv("C:\\Users\\krishnan\\Downloads\\impalleconeu\\LOCATION_20210225_062415.csv")
//  locdata = locdata.select("LOCID","LOCNAME")
//  auditdata = auditdata.join(locdata,auditdata("AUDITLOCATIONID") === locdata("LOCID"),"left_outer")
//  auditdata = auditdata.withColumnRenamed("LOCNAME","AUDITLOCATIONNAME").drop("LOCID")
//  auditdata.show(false)
//  println(" auditdata count " +auditdata.count())
//  println("distant assetids in auditdata " +auditdata.select("ASSETID").distinct().count())
//
//
//  val min_max =  auditdata.select(min("AUDITTIME").as("AUDITSTARTTIME"),max("AUDITTIME").as("AUDITENDTIME"))
//  min_max.show(false)
//  val audit_start_time = min_max.select("AUDITSTARTTIME").first().get(0)
//  println(audit_start_time)
//  val audit_start_date = min_max.withColumn("AUDITSTARTDATE",to_date(col("AUDITSTARTTIME"),"YYYYMMDD")).select("AUDITSTARTDATE").first().get(0)
//  println(audit_start_date)
//
//
//  var asset_journey = spark.read.option("header","true").csv("C:\\Users\\krishnan\\Downloads\\impalleconeu\\AS_AS_VE_FR_1_20210223_061348.csv")
//
//  asset_journey = asset_journey.filter(col("AJSTATUS") =!= "DISCARD")
//    .withColumn("FROMTIME", to_timestamp(col("FROMTIME"),"yyyymmddHHmmss"))
//    .withColumn("AJTOTIME", to_timestamp(col("AJTOTIME")))
//
//  //selected columns
//  asset_journey = asset_journey.select("FROMTIME","ASSETID","AJFROMLOCID","AJTOLOCID","AJTOTIME","AJSTATUS")
//    .filter(col("FROMTIME") >= audit_start_time)
//
//  asset_journey =asset_journey.withColumn("AJTOTIME",
//    when(col("AJSTATUS") === "OPEN",current_timestamp()).otherwise(col("AJTOTIME")))
//
//  asset_journey = asset_journey.join(locdata,asset_journey("AJFROMLOCID") === locdata("LOCID"),"left_outer").select( asset_journey("*") )
//
//  asset_journey.columns:
//
//  asset_journey = asset_journey.withColumnRenamed("LOCNAME","SYSTEMLOCATIONNAME")
//    .withColumnRenamed("AJFROMLOCID","SYSTEMLOCATIONID").drop("LOCID")
//
//  println("asset count from journey" + asset_journey.select("ASSETID").distinct().count())
//
//
//
//  var mappeddata = auditdata.join(asset_journey,"ASSETID").withColumn("BATCHSUBSTATE", lit(null))
//  mappeddata = mappeddata.filter(col("AUDITTIME").between(col("FROMTIME"),col("AJTOTIME")))
//  mappeddata = mappeddata.withColumn("RECONTYPE" ,lit("INNER"))
//
//  println("total mappings count " + mappeddata.count())
//  println("distinct asset mapping count " + mappeddata.select("ASSETID").distinct().count())
//  val found_asset = mappeddata.filter(col("RECONTYPE").isin(Seq("Found Asset","Unmatched Batch State","Unmatched Asset State"):_*))
//
//
//  var left_audit = auditdata.join(asset_journey,asset_journey("ASSETID") === auditdata("ASSETID"),"left_anti")
//    .withColumn("RECONTYPE" ,lit("left_audit"))
//  left_audit.show(false)
//  var left_system = asset_journey.join(auditdata,asset_journey("ASSETID") === auditdata("ASSETID"),"left_anti")
//    .filter(lit(audit_start_time).between(col("FROMTIME"),col("AJTOTIME")))
//    .withColumn("RECONTYPE" ,lit("left_system"))
//  left_system.orderBy("ASSETID","FROMTIME").show(false)
//
//
//
//
//  def proUnion(df1:DataFrame,df2:DataFrame): DataFrame = {
//    val cols1 = df1.columns.toSet
//    val cols2 = df2.columns.toSet
//    val total = cols1 ++ cols2
//    def expr(myCols: Set[String],allCols: Set[String]) = {
//      allCols.toList.map(x => x match {
//        case x if myCols.contains(x) => col(x)
//        case _ => lit(null).as(x)
//      })
//    }
//    df1.select(expr(cols1,total):_*).union(df2.select(expr(cols2,total):_*))
//  }
//
//
//  var filldata = proUnion(mappeddata,left_audit)
//  filldata = proUnion(filldata,left_system)
//  filldata.show(100,false)
//
//  var asset = spark.read.option("header","true").csv("C:\\Users\\krishnan\\Downloads\\impalleconeu\\asset_20210225_034235.csv")
//  asset = asset.select("ASSETID","ASSETTYPE")
//
//  var assettype = spark.read.option("header","true").csv("C:\\Users\\krishnan\\Downloads\\impalleconeu\\assettype_20210225_034622.csv")
//  assettype = assettype.select("ASSETTYPE","ASSETTYPENAME")
//
//  val assettypename= asset.join(assettype,assettype("ASSETTYPE") === asset("ASSETTYPE"),"left_outer").drop(assettype("ASSETTYPE"))
//
//  var new_mappeddata = filldata.join(assettypename,assettypename("ASSETID") === filldata("ASSETID"),"left_outer").drop(assettypename("ASSETID"))
//
//  new_mappeddata = new_mappeddata.withColumn("AUDITASSETSTATE",concat(lit("INUSE")))
//
//  new_mappeddata.show(false)
//  new_mappeddata = new_mappeddata.withColumn("RECONUSER",lit(""))
//
//  new_mappeddata.filter(col("VALIDFROM") >= audit_start_date)
//    .withColumn("VALIDTO", when(col("VALIDTO").isNull,current_date()).otherwise(col("VALIDTO")))
//
//
//  new_mappeddata.withColumn("isduplicate",lit(true))
//  scandata.filter(col("BARCODE") === "8003006625100166831000000052").filter(col("FORMTYPE") === "PHYSICALINVENTORY")
//    .filter(col("rank") =!= 1 && col("lag").isNull ).show(false)
//
//  scandata.na.drop("all")
//
//  val final_one = reconciled.withColumn("LOCID",
//    when(col("RECONTYPE") =!= "Missing Asset",col("AUDITLOCATIONID"))
//      .otherwise(col("SYSTEMLOCATIONID")))
//    .withColumn("AUDITTIME",
//      when(col("AUDITTIME").isNull,col("AUDITSTARTTIME")).otherwise(col("AUDITTIME")))
//
//  scandata.filter(col("BARCODE").isin(S("DC","DX")))
//
//
//
//
//
//val new_left = scandata
//
//  val batchState_NAINBOUND = new_left.filter(col("BATCHSTATE") === "NA" && col("FORMTYPE") === "INBOUND")
//  //batchState_NAINBOUND.show(100,false)
//  val batchState_NAOUTBOUND = new_left.filter(col("BATCHSTATE") === "NA" && col("FORMTYPE") === "OUTBOUND")
//  //batchState_NAOUTBOUND.show(100,false)
//  val batchState_NotNA = new_left.filter(col("BATCHSTATE") =!= "NA")
//  //batchState_NotNA.show(false)
//  val batchState_NAINBOUNDjoined =  batchState_NAINBOUND.join(deliveryitem, deliveryitem("DELIVERYNUMBER") === batchState_NAINBOUND("LASTTRANSACTIONDELNUMBER")
//    && deliveryitem("CONTAINERTYPECODE") === batchState_NAINBOUND("ASSETTYPE") ,"left_outer").drop("DELIVERYNUMBER","DELIVERYITEMID","CONTAINERTYPECODE","SYSTEMBATCHSTATE")
//
//  //    batchState_NAINBOUNDjoined.show(100,false)
//  val batchState_NAOUTBOUNDjoined =  batchState_NAOUTBOUND.join(deliveryitem, deliveryitem("DELIVERYNUMBER") === batchState_NAOUTBOUND("LASTTRANSACTIONDELNUMBER")
//    && deliveryitem("DELIVERYITEMID") === batchState_NAOUTBOUND("LINEITEMID") ,"left_outer").drop("DELIVERYNUMBER","DELIVERYITEMID","CONTAINERTYPECODE","SYSTEMBATCHSTATE")
//  //  batchState_NAOUTBOUNDjoined.show(100,false)
//  val batchState_NAjoined = batchState_NAINBOUNDjoined.union(batchState_NAOUTBOUNDjoined).withColumnRenamed("new_lead_BATCHSTATE","SYSTEMBATCHSTATE")
//  //batchState_NAjoined.show(100,false)
//  val new_left_new =  proUnion(batchState_NAjoined,batchState_NotNA)
//
//
//
//
//  new_left.explain()
//
//
//
//  val final_one = new_left.withColumn("ISRECONCILED", when(col("RECONTYPE") === "Matched" , lit(true)).otherwise(lit(false)))
//    .withColumn("LOCID",when(col("RECONTYPE") =!= "In Inventory, Not Found at Location",col("AUDITLOCATIONID")).otherwise(col("SYSTEMLOCATIONID")))
//    .withColumn("AUDITTIME",when(col("AUDITTIME").isNull,col("AUDITSTARTTIME")).otherwise(col("AUDITTIME"))).withColumn("AUDITASSETSTATE",when(col("RECONTYPE") === "In Inventory, Not Found at Location",lit("Unknown")).otherwise(lit("INUSE"))).withColumn("ISDEDUPLICATE",lit(false)).withColumn("ISDUPLICATE",when(col("ISDUPLICATE").isNull,lit(false)).otherwise(col("ISDUPLICATE"))).withColumn("ISUPLOADED",lit(false)).select("AUDITSTARTTIME","AUDITLOCATIONNAME","AUDITLOCATIONID","AUDITBATCHSTATE","AUDITASSETSTATE","SYSTEMLOCATIONID","SYSTEMLOCATIONNAME","SYSTEMBATCHSTATE","SYSTEMASSETSTATE","RECONTYPE","LASTTRANSACTIONDELNUMBER","LASTTRANSACTIONDATE","LASTTRANSACTIONACTIVITY","AUDITTIME","ASSETTYPENAME","ASSETTYPE","ASSETID","LOCID","RECONID","ISDUPLICATE","ISRECONCILED","ISUPLOADED","ISDEDUPLICATE")
//
//
//
//
//  val audit_start_date = min_max.orderBy("AUDITSTARTTIME").withColumn("AUDITSTARTDATE",to_date(col("AUDITSTARTTIME")))
//    .select("AUDITSTARTDATE")..get(0)
//  val audit_start_time = min_max.join
//
//}
//
