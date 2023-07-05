package org.mt.assignments
package acceletator

import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession

object deltaUpload extends App{

  val spark:SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("SparkByExamples.com")
    .getOrCreate()

 val  accessKeyId="AKIAV3QP7XCB6WEBBJVX"
 val  secretAccessKey="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"

  spark.sparkContext
    .hadoopConfiguration.set("fs.s3a.access.key", accessKeyId)
  spark.sparkContext
    .hadoopConfiguration.set("fs.s3a.secret.key", secretAccessKey)
//  spark.sparkContext
//    .hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")
   // s3.us-east-2.amazonaws.com



 val dat1 = spark.read.option("header","true").option("inferSchema","true").csv("s3a://raw-data-source-athena-crud/stack-overflow-developer-survey-2020/survey_results_public.csv")
 dat1.show(false)

 dat1.write.format("delta").save("s3a://delta-data-athena-crud/stack-overflow-developer-survey-2020")

 val deltaTable = DeltaTable.forPath("s3a://delta-data-athena-crud/stack-overflow-developer-survey-2020")
  deltaTable.generate("symlink_format_manifest")




 // data.write.format("delta").save("C:\\Users\\krishnan\\Downloads\\test_delta")
  //dat1.write.format("delta").save("C:\\Users\\krishnan\\Downloads\\test_delta2")
//  dat1.write.mode("append").saveAsTable("lol1")
//  val ddd = spark.read.table("lol1")
//  ddd.show(false)

// val deltaTable = DeltaTable.forPath("C:\\Users\\krishnan\\Downloads\\test_delta")
//  deltaTable.generate("symlink_format_manifest")

}
