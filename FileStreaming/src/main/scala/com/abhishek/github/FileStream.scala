package com.abhishek.github
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.streaming.Trigger

object FileStream extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    //Initialise Spark Session
    val spark = SparkSession.builder()
      .master("local[3]")
      .appName("File Streaming Model")
      .config("spark.streaming.stopGracefullyOnShutdown","true")
      .config("spark.sql.streaming.schemaInference","true") // to enable schema Inference
      .getOrCreate()

    //Reading a FileStream is almost similar to reading a file
    //to get a data stream reader
    val rawDataFrame = spark.readStream
      //format is json as data is coming in form of json
      .format("json")
     //set some options where input is the folder name in current directory
      .option("path","input")
      //Trigger 1 file at a time
      .option("maxFilesPerTrigger",1)
      //load the data which will return a streaming data frame
      .load()

    //rawDataFrame.printSchema()

    //Select Query to query over the Json
    val explodeDataFrame  = rawDataFrame.selectExpr("InvoiceNumber","CreatedTime","StoreID","PosID",
      "CustomerType","PaymentMethod","DeliveryType","DeliveryAddress.City",
      "DeliveryAddress.State","DeliveryAddress.PinCode",
      "explode(InvoiceLineItems) as LineItem")//as InvoiceLineItems is an Array we will explode over it to get the items

    //explodeDataFrame.printSchema()

    //Now flatten the record for LineItem
    val flattenDataFrame = explodeDataFrame
      .withColumn("ItemCode", expr("LineItem.ItemCode"))
      .withColumn("ItemDescription",expr("LineItem.ItemDescription"))
      .withColumn("ItemPrice",expr("LineItem.ItemPrice"))
      .withColumn("ItemQty",expr("LineItem.ItemQty"))
      .withColumn("TotalValue",expr("LineItem.TotalValue"))
      .drop("LineItem")

    //Create a writerQuery to write the flatten DataFrame
    val writeQuery = flattenDataFrame
      .writeStream
      .format("json")
      .option("path","output")
      .option("checkpointLocation","chk-point-dir")
      .outputMode("append")
      .queryName("Flatten Invoice")
      //Set a trigger for 1 minute
      .trigger(Trigger.ProcessingTime("1 minute"))
      .start()

    logger.info("FLattend Invoice Writter Started")
    writeQuery.awaitTermination()



  }

}
