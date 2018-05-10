package com.datamantra.logs_analyzer.sparkstreaming

import java.io.InputStream
import java.util.Properties

import com.datamantra.logs_analyzer.offsetsource.KafkaSource
import com.datamantra.logs_analyzer.offsetsource.stores.ZooKeeperOffsetsStore
import com.datamantra.logs_analyzer.utils.AccessLogUtils
import com.datamantra.utils.ApacheAccessLog
import com.datastax.spark.connector.SomeColumns
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


/**
 * The LogAnalyzerStreaming illustrates how to use logs with Spark Streaming to
 *   compute statistics every slide_interval for the last window length of time.
 *
 * To feed the new lines of some logfile into a socket, run this command:
 *   % tail -f [YOUR_LOG_FILE] | nc -lk 9999
 *
 * If you don't have a live log file that is being written to, you can add test lines using this command:
 *   % cat ../../data/apache.access.log >> [YOUR_LOG_FILE]
 *
 * Example command to run:
 * % spark-submit
 *   --class "com.databricks.apps.logs.chapter1.LogAnalyzerStreaming"
 *   --master local[4]
 *   target/scala-2.10/spark-logs-analyzer_2.10-1.0.jar
 */
object LogAnalyzerKafkaStreaming {
  //val WINDOW_LENGTH = new Duration(30 * 1000)
  val SLIDE_INTERVAL = new Duration(5 * 1000)
  val BATCH_INTERVAL = new Duration(5 * 1000)


  def main(args: Array[String]) {


    val input : InputStream = getClass.getResourceAsStream("/loganalysis.properties")
    val prop: Properties = new Properties
    prop.load(input)

    val cassandraKeySpace = prop.getProperty("com.loganalysis.app.cassandra.keyspace")
    val lookupFile = prop.getProperty("com.loganalysis.app.spark.lookup.file")
    val topic = prop.getProperty("com.loganalysis.app.kafka.topic")

    val conf = new SparkConf()
    .setMaster(prop.getProperty("com.loganalysis.app.spark.master"))
    .setAppName(prop.getProperty("com.loganalysis.app.spark.app.name"))
    .set("spark.cassandra.connection.host", prop.getProperty("com.loganalysis.app.cassandra.host"))
    .set("spark.streaming.kafka.maxRatePerPartition", prop.getProperty("com.loganalysis.app.spark.streaming.kafka.maxRatePerPartition"))
    .set("spark.cassandra.output.batch.size.bytes", prop.getProperty("com.loganalysis.app.cassandra.output.batch.size.bytes")) //8000 * 1024
    .set("spark.cassandra.output.concurrent.writes", prop.getProperty("com.loganalysis.app.cassandra.output.concurrent.writes"))
    //.set("spark.cassandra.output.consistency.level", prop.getProperty("com.loganalysis.app.cassandra.output.consistency.level"))
    .set("spark.cassandra.output.batch.grouping.key", prop.getProperty("com.loganalysis.app.cassandra.output.batch.grouping.key")) ///replica_set/partition
    .set("spark.cassandra.sql.keyspace", s"$cassandraKeySpace")



    val sc = new SparkContext(conf)
    val countryCodeBrodcast = sc.broadcast(AccessLogUtils.createCountryCodeMap(lookupFile))
    val ssc = new StreamingContext(sc, BATCH_INTERVAL)


    val kafkaParams = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> prop.getProperty("com.loganalysis.app.kafka.bootstrap.servers"),
      ConsumerConfig.GROUP_ID_CONFIG -> prop.getProperty("com.loganalysis.app.spark.groupid"),
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> prop.getProperty("com.loganalysis.app.kafka.auto.offset.reset.config")
    )

        // For auto.offset.commit This is not required...
    val offsetStore = new ZooKeeperOffsetsStore("localhost", "/kafka")


    val logLinesDStream = KafkaSource.kafkaStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, offsetStore, topic)
                                    .map(_._2)

   /*
    Receiver Based Streaming application
    val logLinesDStream = KafkaUtils.createStream(
      ssc,            // StreamingContext object
      zkQuorum,
      clientGroup,
      topicMap  // Map of (topic_name -> numPartitions) to consume, each partition is consumed in its own thread
      // StorageLevel.MEMORY_ONLY_SER_2 // Storage level to use for storing the received objects
    ).map(_._2)
   */



    //Error Handling and ETL Process
    val accessLogsDStream = logLinesDStream.map(ApacheAccessLog.parseLogLine)
    val accessLogs = accessLogsDStream.filter(_.isRight).map(_.right.get)
    val errorRecords = accessLogsDStream.filter(_.isLeft).map(_.left.get._2)
    val refinedLogs = accessLogs.map(log => AccessLogUtils.getViewedProducts(log, countryCodeBrodcast)).cache()


    import com.datastax.spark.connector.streaming._

      // Actual Analytics.
      refinedLogs.map(vp => (vp.products, 1))
                 .reduceByKey(_ + _)
                 .saveToCassandra(cassandraKeySpace, "page_views", SomeColumns("page", "views"))

      refinedLogs.map(vp => (vp.responseCode, 1))
                 .reduceByKey(_ + _)
                 .saveToCassandra(cassandraKeySpace, "status_counter", SomeColumns("status_code", "count"))

      refinedLogs.map(vp => (vp.cntryCode, 1))
                 .reduceByKey(_ + _)
                 .saveToCassandra(cassandraKeySpace, "visits_by_country", SomeColumns("country", "count"))

    /*
    refinedLogs.foreachRDD(vpRdd => {

      val productViewCount = vpRdd.map(vp => vp.products)
                                  .countByValue()
      productViewCount.foreach(t => logEventDAO.updatePageViews(t._1, t._2.toInt))

      val responseCodeCounts = vpRdd.map(vp => vp.responseCode)
                                         .countByValue()
      responseCodeCounts.foreach(result => logEventDAO.updateStatusCounter(result._1, result._2))

      val productViewsByCntry = vpRdd.map(vp => vp.cntryCode)
                                          .countByValue()
      productViewsByCntry.foreach(result => logEventDAO.updateVisitsByCountry(result._1, result._2.toInt))

      val logVolumePerMinute = vpRdd.map(vp => AccessLogUtils.getMinPattern(vp.dateTime))
                                         .countByValue()
      logVolumePerMinute.foreach(result => logEventDAO.updateLogVolumeByMinute(result._1, result._2.toInt))

    })

*/
    ssc.start()
    ssc.awaitTermination()
  }
}
