package com.esoftwarelabs.aqpredictions

import java.nio.ByteBuffer

import com.amazonaws.auth.{BasicAWSCredentials, DefaultAWSCredentialsProviderChain}
import com.amazonaws.regions.RegionUtils
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import org.apache.log4j.{Level, Logger}

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions
import org.apache.spark.streaming.kinesis.KinesisUtils
import java.util.logging.Logging

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._
import org.apache.spark.streaming.Time

//import org.apache.spark.Logging
import org.apache.spark.internal.Logging

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.regression.DecisionTreeRegressor
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.DenseVector


object AirQualityPredictions {
  def main(args: Array[String]) {
    // Check that all required args were passed in.
    if (args.length != 6) {
      System.err.println(
        """
          |Usage: KinesisWatch <app-name> <stream-name> <endpoint-url> <batch-length-ms> <inputDelimiter> <models3bucket>
          |
          |    <app-name> is the name of the consumer app, used to track the read data in DynamoDB
          |    <stream-name> is the name of the Kinesis stream
          |    <endpoint-url> is the endpoint of the Kinesis service
          |                   (e.g. https://kinesis.us-east-1.amazonaws.com)
          |    <batch-length-ms> How long is each batch for the temp table view directly from stream in milliseconds
          |    <input-Delimiter> Delimiter? - how to split the data in each row of the stream to generate fields i.e ","
          |    <model-s3bucket>  Name of the s3 bucket where you have stored your model from Model generate program 
          |     
        """.stripMargin)
      System.exit(1)
    }
        
    // Populate the appropriate variables from the given args
    val Array(appName, streamName, endpointUrl, batchLength, inputDelimiter, models3bucket ) = args

    // Determine the number of shards from the stream using the low-level Kinesis Client
    // from the AWS Java SDK.
    val credentials = new DefaultAWSCredentialsProviderChain().getCredentials()
    require(credentials != null,
      "No AWS credentials found. Please specify credentials using one of the methods specified " +
        "in http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/credentials.html")
    val kinesisClient = new AmazonKinesisClient(credentials)
    kinesisClient.setEndpoint(endpointUrl)
    val numShards = kinesisClient.describeStream(streamName).getStreamDescription().getShards().size

    // In this example, we're going to create 1 Kinesis Receiver/input DStream for each shard.
    // This is not a necessity; if there are less receivers/DStreams than the number of shards,
    // then the shards will be automatically distributed among the receivers and each receiver
    // will receive data from multiple shards.
    val numStreams = numShards

    // Spark Streaming batch interval
    val batchInterval = Milliseconds(batchLength.toLong)

    // Kinesis checkpoint interval is the interval at which the DynamoDB is updated with information
    // on sequence number of records that have been received. Same as batchInterval for this
    // example.
    val kinesisCheckpointInterval = batchInterval

    // Get the region name from the endpoint URL to save Kinesis Client Library metadata in
    // DynamoDB of the same region as the Kinesis stream
    
    val regionName = RegionUtils.getRegionByEndpoint(endpointUrl).getName
    
    // Setup the SparkConfig and StreamingContext
    val sparkConf = new SparkConf().setAppName(appName)
    val ssc = new StreamingContext(sparkConf, batchInterval)
        
    // Create the Kinesis DStreams
    val kinesisStreams = (0 until numStreams).map { i =>
      KinesisUtils.createStream(ssc, appName, streamName, endpointUrl, regionName,
        InitialPositionInStream.LATEST, kinesisCheckpointInterval, StorageLevel.MEMORY_AND_DISK_2)
    }

    // Union all the streams
    val unionStreams = ssc.union(kinesisStreams)

    //define the schema for input data structure
    val schemaString = "deviceid latitude longitude sensorreading seconds"
    //Passing the schemaString as a part of the arguments seperated by spaces.
    
    val warehouseLocation = "file:${system:user.dir}/spark-warehouse"
    val spark = SparkSession
      .builder
      .config(sparkConf)
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()
    
    val sqlContext = spark.sqlContext
        import sqlContext.implicits._ 
        import org.apache.spark.sql.functions._ 
      
    val tableSchema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
    
    // Drop the tables if it already exists 
    spark.sql("DROP TABLE IF EXISTS tbl_airqualityPredictions")
   
    // Create the tables to store your streams 
    spark.sql("CREATE TABLE tbl_airqualityPredictions(deviceid string,latitude string,longitude string,time string,sensorreading string,prediction string,airqualityprediction string) STORED AS TEXTFILE")
    
    unionStreams.foreachRDD ((rdd: RDD[Array[Byte]], time: Time) => {

        val rowRDD = rdd.map(w => Row.fromSeq(new String(w).split(inputDelimiter)))
           
        val sensorreadingsDF = spark.createDataFrame(rowRDD,tableSchema)
        
        val featuresDF = sensorreadingsDF.select($"deviceid",$"latitude",$"longitude",$"seconds",vectorize($"sensorreading",$"latitude",$"longitude",$"seconds").as("features"))
        
        val PIPELINE_MODEL: String = models3bucket
          
        val model: PipelineModel = PipelineModel.load(PIPELINE_MODEL)
        
        val airqualityresultsDF = model.transform(featuresDF)
            
        val lblairqualityresultsDF = airqualityresultsDF.withColumn("AirQuality", getText($"predictedLabel"))
        
        val vectorToColumn = udf{ (x:DenseVector, index: Int) => x(index) }
    
        val finalresultsDF = lblairqualityresultsDF.withColumn("SensorReading",vectorToColumn(col("features"),lit(0)))
    
        finalresultsDF.createOrReplaceTempView("airqualityTempTable")
        
        //Insert continuous streams into hive table
        spark.sql("insert into table tbl_airqualityPredictions select deviceid,latitude,longitude,time,SensorReading,prediction,AirQuality from airqualityTempTable")
        
        // select the parsed messages from table using SQL and print it (since it runs on drive display few records)
        val sqllblairqualityresultsDF = spark.sql("select * from airqualityTempTable")
        println(s"========= $time =========")
        sqllblairqualityresultsDF.show()
      
    })
    
    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  
  }
  
  import org.apache.spark.sql.functions.udf
  val vectorize = udf( (s:Double, s1:Double, s2:Double, s3:Double) => Vectors.dense(Array[Double](s, s1, s2, s3)) )
  val getText = udf( (value: Int) => AirQualityType.fromPrediction(value) )
}