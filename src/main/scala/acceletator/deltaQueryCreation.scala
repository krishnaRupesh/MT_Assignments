package org.mt.assignments
package acceletator

import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

object deltaQueryCreation extends App{

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

val s3_delta_path = "s3a://delta-data-athena-crud/stack-overflow-developer-survey-2020"

    val dat1 = spark.read.format("delta").load("s3a://delta-data-athena-crud/stack-overflow-developer-survey-2020")
   // dat1.show(false)

  val statement  = spark.sql(s"select * delta.'$s3_delta_path'").collect()

  print(statement)

  dat1.write.format("delta").saveAsTable()

}
