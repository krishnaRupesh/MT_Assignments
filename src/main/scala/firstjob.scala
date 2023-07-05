
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.UserDefinedFunction

import org.apache.hudi.QuickstartUtils._
import scala.collection.JavaConversions._
import org.apache.spark.sql.SaveMode._
import org.apache.hudi.DataSourceReadOptions._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieWriteConfig._

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


object firstjob extends App{


  val sc = new SparkContext(master = "local", appName = "dada")

  val data = sc.textFile("C:\\Users\\krishnan\\Documents\\NomuPay Interview Questions.txt")

  val data2 = data.collect()

  println(data2)



  val spark = SparkSession.builder().master("local").appName("rupesh")
    .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
   // .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.hudi.catalog.HoodieCatalog")
    .getOrCreate()



  //spark.conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
  def journey_vs_closeddwellview(CLOSEDDWELLVIEW_df :DataFrame,ASSETJOURNEY_df:DataFrame): (DataFrame,Int) = {

    val dwellview = CLOSEDDWELLVIEW_df.groupBy("assetid").agg(count("*").as("count")).withColumn("NETWORK_OBJ",lit("CLOSEDDWELLVIEW"))
    val closed_journey = ASSETJOURNEY_df.filter(col("ajstatus") === "CLOSED").filter(col("asmstateid").isin("DX","DC")).groupBy("assetid").agg(count(lit(1)).as("count"),sum("sad")).withColumn("NETWORK_OBJ",lit("ASSETJOURNEY"))

    val only_dwell = dwellview.join(closed_journey,Seq("assetid","count"),"leftanti")
    val only_closed_journey = closed_journey.join(dwellview,Seq("assetid","count"),"leftanti")

    val error_data = only_dwell.unionAll(only_closed_journey)

    val new_data = error_data.filter(col("assetid") =!= "dadta")

    val count1 = error_data.count().toInt

    error_data.distinct()

    (error_data,count1)
  }

//  trait b
//  class A extends b{
//  def readdat (df :DataFrame): (DataFrame,Int) = {
//
//    df.show(5,false)
//    val count = df.count().toInt
//    (df,count)
//  }
//
//  print(spark.conf.get("spark.sql.catalog.spark_catalog"))
//  import spark.implicits._
//  val df =
//    Seq(("2022-01-01 00:00:00", 1),
//      ("2022-01-01 00:15:00", 1),
//      ("2022-01-08 23:30:00", 1),
//      ("2022-01-22 23:30:00", 4)).toDF("date","a")
//
//
//  val df0 =
//    df.groupBy(window(col("date"), "1 week", "1 week", "0 minutes"))
//      .agg(sum("a") as "sum_a")
//
//  df0.show(false)
//
//  val df1 = df0.select("window.start", "window.end", "sum_a")
//
////  df1.filter(!(col("BARCODE") rlike "^([A-Z]|[0-9]|[a-z])+$")).show(false)
//
// // df1.show()





//
//   val data = spark.read.option("header","true").csv("C:\\Users\\krishnan\\Downloads\\transactiontypes_20220301_121750.csv")
////
//  data.show(false)
//  val dataGen = new DataGenerator
//  val inserts = convertToStringList(dataGen.generateInserts(10))
//  val df = spark.read.json(spark.sparkContext.parallelize(inserts, 2))
//
//  df.show(false)
//  df.write.mode("overwrite").format("hudi")
//  .option("hoodie.table.name", "data_table")
//    .options(getQuickstartWriteConfigs).
//    option(PRECOMBINE_FIELD_OPT_KEY, "ts").
//    option(RECORDKEY_FIELD_OPT_KEY, "uuid").
//  //  option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath").
//    mode(Overwrite)
//    .save("C:\\Users\\krishnan\\Downloads\\hudi_test")
////
// //data.write.mode("overwrite").parquet("C:\\Users\\krishnan\\Downloads\\txn")
//  data.write.mode("overwrite").option("header","true").csv("C:\\Users\\krishnan\\Downloads\\txn1")
//
////data.sqlContext
//  val a = new A
//
//  val method = a.getClass.getMethod("readdat",data.getClass)
//  val (new_data:DataFrame,numb) = method.invoke(a,data)
//  data.filter(col("SCANTTYPEID").isNull && col("SCANLOCID").isNotNull).show(false)
//
//  new_data.show(false)
//
//  import org.apache.spark.sql.expressions.Window
//
//
//  val windowbatch = Window.partitionBy("ASSETID").orderBy(desc("FROMTIME"))
//  data.withColumn("dad",count("*") over windowbatch )
//
//




//  val actual_value = 0
//  val expected_value = 2
//  data.createOrReplaceTempView("error_data")
//


















//  val  summation  = (timelist:List[Double]) => {
//    var sum = 0.0
//    for (i <- timelist){
//      if (i==0.0){
//        sum = 0
//      }
//      else if(sum > 120) {
//        sum = 0
//      }
//      else {
//        sum = sum + i
//      }
//    }
//    sum
//  }
//
//  val udfsumation:UserDefinedFunction = udf(summation)
}

