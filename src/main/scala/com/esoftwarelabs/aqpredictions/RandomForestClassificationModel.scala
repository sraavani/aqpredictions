package com.esoftwarelabs.aqpredictions

import java.nio.ByteBuffer

import com.amazonaws.auth.{ BasicAWSCredentials, DefaultAWSCredentialsProviderChain }
import com.amazonaws.regions.RegionUtils
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import org.apache.log4j.{ Level, Logger }

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{ Milliseconds, StreamingContext }
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions
import org.apache.spark.streaming.kinesis.KinesisUtils
import java.util.logging.Logging

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.Time

//import org.apache.spark.Logging
import org.apache.spark.internal.Logging

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.tree.impl.RandomForest

import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{ IndexToString, StringIndexer, VectorIndexer }
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.feature.VectorAssembler


object RandomForestClassificationModel {
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
          |    <inputDelimiter> Delimiter? - how to split the data in each row of the stream to generate fields i.e ","
          |    <models3bucket> Name of the s3 bucket where you store your model           
          |     
        """.stripMargin)
      System.exit(1)
    }

    // Populate the appropriate variables from the given args
    val Array(appName, streamName, endpointUrl, batchLength, inputDelimiter, models3bucket) = args

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
    val regionName = RegionUtils.getRegionByEndpoint(endpointUrl).getName()

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
    val schemaString = "deviceid latitude longitude sensordata seconds"
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

    unionStreams.foreachRDD((rdd: RDD[Array[Byte]], time: Time) => {
      if (rdd.take(1).size == 1) {
        val rowRDD = rdd.map(w => Row.fromSeq(new String(w).split(inputDelimiter)))

        val sensorreadingsDF = spark.createDataFrame(rowRDD, tableSchema)

        val featuresDF = sensorreadingsDF.select($"deviceid", $"latitude", $"longitude", $"seconds", getLabel($"sensordata").as("label"), vectorize($"sensordata",$"latitude",$"longitude",$"seconds").as("feature"))

        trainModel(featuresDF, models3bucket)

      }
    })

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }

  def trainModel(featuresDF: org.apache.spark.sql.DataFrame, models3bucket: String) {
    val assembler = new VectorAssembler()
      .setInputCols(Array("feature"))
      .setOutputCol("features")

    val sampleData = assembler.transform(featuresDF)

    // Index labels, adding metadata to the label column.
    // Fit on whole dataset to include all labels in index.
    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(sampleData)

    // Automatically identify categorical features, and index them.
    // Here, we treat features with > 4 distinct values as continuous.
    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4)
      .fit(sampleData)

    // Split the data into training and test sets (40% held out for testing).
    val Array(trainingData, testData) = sampleData.randomSplit(Array(0.6, 0.4))

    // Train a RandomForest model.
    val rf = new RandomForestClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")
      .setNumTrees(10)

    // Convert indexed labels back to original labels.
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    // Chain indexers and forest in a Pipeline.
    val pipeline = new Pipeline()
      .setStages(Array(labelIndexer, featureIndexer, rf, labelConverter))

    // Train model. This also runs the indexers.
    val model = pipeline.fit(trainingData)

    // Make predictions.
    val predictions = model.transform(testData)

    // Select (prediction, true label) and compute test error.
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val accuracy = evaluator.evaluate(predictions)

    println("Accuracy on test data = " + accuracy)

    model.save(models3bucket)

    // Select example rows to display.
    predictions.select("predictedLabel", "label", "features").show(5)
  }

  import org.apache.spark.sql.functions.udf

  val getLabel = udf((sensorData: Int) => sensorData match {
    case reading if reading < 100 => 0
    case reading if reading >= 100 && reading < 400 => 1
    case reading if reading >= 400 && reading < 600 => 2
    case reading if reading >= 600 => 3
  })

  val vectorize = udf( (s:Double, s1:Double, s2:Double, s3:Double) => Vectors.dense(Array[Double](s, s1, s2, s3)) )

}