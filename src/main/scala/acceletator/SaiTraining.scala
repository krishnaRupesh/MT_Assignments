package org.mt.assignments
package acceletator

import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{monotonicallyIncreasingId, monotonically_increasing_id}

import sys.process._

object SaiTraining {

  def main(args: Array[String]):Unit = {

    println("=============Start program============")

    val lsin = List(1,2,34,5,6,78,8)

    lsin.foreach(println)

    val newlist = lsin.filter(x => x >10)

    newlist.foreach(println)

    monotonically_increasing_id
    monotonicallyIncreasingId

    val spark =  SparkSession.builder().appName("test").master("local[*]").getOrCreate()
    val data  = spark.read.csv("")
    val schema = data.withColumn("unique",lit(1L)).schema


    data.select(max("type")).first().getInt(0)

    data.withColumn("sd", when(col("sds") === "dsd", lit("sd")).when(col("sds").isNull ,lit("sf")))

    data.withColumn("ddad",concat_ws("-",split(col("sds")," ")))

    data.join(data.hint("broadcast"), Seq("STOCKTYPE"),"leftanti")
      .withColumn("dada",when(col("dsd").like("dad%"),lit(true)).otherwise())

    data.withColumn("ada",to_timestamp(lit("asa"))).withColumn("sds",)

    val uuid = udf(() => java.util.UUID.randomUUID().toString)
    data.withColumn("uniqueId", uuid()).show(false)

    val maxid = {
      if(data.isEmpty) data else data.isEmpty
    }

    data.filter(col("FORMTYPE").notEqual("asa","asas"))
    import org.apache.spark.sql.types.{StringType, StructField, StructType,BooleanType}
    import org.apache.spark.sql.Row

    val schema = StructType( Array(StructField("FORMTYPE", StringType,true),StructField("LIFECYCLESTATEID", StringType,true),StructField("CREATEJOURNEY", BooleanType,true),StructField("IGNORETXN", BooleanType,true),StructField("REQUIRECORRECTION", BooleanType,true),StructField("CREATEASSET", BooleanType,true)))

    val rowData  = Seq(Row("CUSTOMERRECONCILIATION","INUSE",true,false,false,false),Row("CUSTOMERAUDIT","INUSE",false,false,false,true),Row("TAGREPLACEMENT","INUSE",true,false,true,true),Row("CUSTOMEROUTBOUND","INUSE",true,false,true,true),Row("PLANTACTIVITY","INUSE",true,false,true,true),Row("CUSTOMERINBOUND","INUSE",true,false,true,true),Row("PHYSICALINVENTORY","SCRAP",false,false,false,true),Row("REPAIR","INUSE",true,false,true,true),Row("PHYSICALINVENTORY","INUSE",false,false,false,true),Row("INBOUND","INUSE",true,false,true,true),Row("DAMAGE","INUSE",true,false,true,true),Row("PLANTRECONCILIATION","INUSE",true,false,false,false),Row("BIRTH","BIRTH",true,false,true,true),Row("OUTBOUND","INUSE",true,false,true,true))

    val dfFromData3 = spark.createDataFrame(rowData,schema)

    val outboundtxnid = if(SCANFORM_df.filter(col("FORMTYPE").isNotNull).isEmpty) {

    } maxtxntypeid+2 else SCANFORM_df.filter(col("FORMTYPE") === "OUTBOUND").select("TXNTYPEID").first().getString(0).toLong

  }



}
