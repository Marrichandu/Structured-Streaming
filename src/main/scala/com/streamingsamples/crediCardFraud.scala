package com.streamingsamples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger



object crediCardFraud extends Serializable{
 /* case class TransactionLogs (CustomerID:String, CreditCardNo:Long, TransactionLocation:String,
                             TransactionAmount:Int, TransactionCurrency:String, MerchantName:String,
                              NumberofPasswordTries:Int, TotalCreditLimit:Int, CreditCardCurrency:String )*/


  def main(args:Array[String]):Unit={
    val spark=SparkSession.builder()
      .master("local[1]")
      .appName("Streaming application")
      .config("spark.streaming.stopGracefulOnShutdown","true")
      .config("spark.sql.streaming.schemaInference","true")
      .getOrCreate()
    import spark.implicits._

    //reading data as an input stream
    val creditCardData=spark.readStream
      .format("csv")
      .option("inferSchema","true")
      .option("path","input")
      //.option("maxFilesPerTrigger",1)
      .option("latestFirst","true")
      .option("cleanSource","delete")
      .load()

    // rename columns as required
    val newDf=creditCardData.withColumnRenamed("_c0","CustomerId")
      .withColumnRenamed("_c1","CreditCardNo")
      .withColumnRenamed("_c2","TransactionLocation")
      .withColumnRenamed("_c3","TransactionAmount")
      .withColumnRenamed("_c4","TransactionCurrency")
      .withColumnRenamed("_c5","MerchantName")
      .withColumnRenamed("_c6","NumberofPasswordTries")
      .withColumnRenamed("_c7","TotalCreditLimit")
      .withColumnRenamed("_c8","CreditCardCurrency")


    // Filters credit card fraud transactions based on the Transactions data

    newDf.printSchema()
    val creditCardFraud=newDf.select("CustomerID","CreditCardNo","TransactionAmount", "TransactionCurrency", "NumberofPasswordTries",
      "TotalCreditLimit", "CreditCardCurrency")
      .filter("NumberofPasswordTries > 3 OR TransactionCurrency != CreditCardCurrency OR ( TransactionAmount * 100.0 / TotalCreditLimit ) > 50")


    //writing the data back to output stream
    val df=creditCardFraud.writeStream
        .format("csv")
        .outputMode("append")
        .option("path","output")
        .option("checkpointLocation","chk-point-dir")
        .trigger(Trigger.ProcessingTime("1 minute"))
        .start()

    df.awaitTermination()



  }
}
