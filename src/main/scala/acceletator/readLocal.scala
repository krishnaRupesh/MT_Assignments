package org.mt.assignments
package acceletator

import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession

object readLocal extends App{

  val spark:SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("SparkByExamples.com")
    .getOrCreate()

 val dat1 = spark.read.option("header","true").option("inferSchema","true").csv("C:\\Users\\krishnan\\Downloads\\stack-overflow-developer-survey-2020\\survey_results_public.csv")
 dat1.show(false)



}
