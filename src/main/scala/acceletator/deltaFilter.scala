package org.mt.assignments
package acceletator

import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

object deltaFilter extends App{

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


 val s3location = "s3://delta-data-athena-crud/stack-overflow-developer-survey-2020"

 val dat1 = spark.read.format("delta").load("s3a://delta-data-athena-crud/stack-overflow-developer-survey-2020")
 dat1.show(false)



// dat1.createOrReplaceTempView("stack_overflow_developer_survey")
//
// val dat2 = spark.sql("select * from stack_overflow_developer_survey where Age = 30")
//
// dat2.show(false)
//
// dat2.write.format("delta").mode("overwrite").save("s3a://delta-data-athena-crud/stack-overflow-developer-survey-2020")
//
// val deltaTable = DeltaTable.forPath("s3a://delta-data-athena-crud/stack-overflow-developer-survey-2020")
//  deltaTable.generate("symlink_format_manifest")

 printAthenaCreateTable(dat1,"asas",s3location)

 def printAthenaCreateTable(df: DataFrame, athenaTableName: String, s3location: String): String = {
  val fields = df.schema.map { (f: StructField) =>
   s"${f.name} ${sparkTypeToAthenaType(f.dataType.toString)}"
  }

//  println(s"CREATE EXTERNAL TABLE IF NOT EXISTS $athenaTableName(")
//  println("  " + fields.mkString(",\n  "))
//  println(")")
//  println("ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'")
//  println("STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat'")
//  println("OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'")
//  println(s"LOCATION '$s3location/_symlink_format_manifest/'")

  val output = s"CREATE EXTERNAL TABLE IF NOT EXISTS $athenaTableName(" + "\n" +
    "  " + fields.mkString(",\n  ") + ")" + "\n" +
    "ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'" + "\n" +
    "STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat'" + "\n" +
    "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'" + "\n" +
    s"LOCATION '$s3location/_symlink_format_manifest/'"

  output
 }

 private def sparkTypeToAthenaType(sparkType: String): String = {
  sparkType match {
   case "StringType" => "STRING"
   case "IntegerType" => "INT"
   case "DateType" => "DATE"
   case "DecimalType" => "DECIMAL"
   case "FloatType" => "FLOAT"
   case "LongType" => "BIGINT"
   case "TimestampType" => "TIMESTAMP"
   case _ => "STRING"
  }

  // data.write.format("delta").save("C:\\Users\\krishnan\\Downloads\\test_delta")
  //dat1.write.format("delta").save("C:\\Users\\krishnan\\Downloads\\test_delta2")
  //  dat1.write.mode("append").saveAsTable("lol1")
  //  val ddd = spark.read.table("lol1")
  //  ddd.show(false)

  // val deltaTable = DeltaTable.forPath("C:\\Users\\krishnan\\Downloads\\test_delta")
  //  deltaTable.generate("symlink_format_manifest")

 }}
