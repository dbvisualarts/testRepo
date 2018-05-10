package com.datamantra.sparkstreaming

import java.io.InputStream
import java.util.Properties


import com.datamantra.logs_analyzer.offsetsource.KafkaSource
import com.datamantra.logs_analyzer.offsetsource.stores.ZooKeeperOffsetsStore
import com.datamantra.logs_analyzer.utils.AccessLogUtils
import com.datamantra.utils.ApacheAccessLog
import com.datastax.spark.connector.SomeColumns
import io.confluent.kafka.serializers.KafkaAvroDecoder
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import loganalysis.avro.ApacheLogEvents
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.sql.SQLContext
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
object LogAnalyzerAvroKafkaStreaming {
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
        "io.confluent.kafka.serializers.KafkaAvroDeserializer",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> prop.getProperty("com.loganalysis.app.kafka.auto.offset.reset.config"),
      "schema.registry.url" -> "http://localhost:8081"
    )

        // For auto.offset.commit This is not required...
    val offsetStore = new ZooKeeperOffsetsStore("localhost", "/kafka")



    var topicsSet = Set[String]()
    topicsSet += topic


    val storedOffsets = offsetStore.readOffsets(topic)
    val kafkaStream = storedOffsets match {
      case None =>
        // start from the latest offsets
        KafkaUtils.createDirectStream[String, Object, StringDecoder, KafkaAvroDecoder](ssc, kafkaParams, topicsSet)
      case Some(fromOffsets) =>
        // start from previously saved offsets
        val messageHandler = (mmd: MessageAndMetadata[String, Object]) => (mmd.key, mmd.message)
        KafkaUtils.createDirectStream[String, Object, StringDecoder, KafkaAvroDecoder, (String, Object)](ssc, kafkaParams, fromOffsets, messageHandler)
    }


    //Saving the offset before processing and saving data. There could be data loss on restarting the streaming app
    /*
    kafkaStream.foreachRDD(rdd => {
      offsetStore.saveOffsets(topic, rdd)
    })
    */


///Processing of data starts here
    val logLinesDStream = kafkaStream.transform(rdd => {

      //rdd.map(_._2.toString).foreach(println)
      val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
      import  sqlContext.implicits._

      val value = rdd.map(_._2.toString)
      val df = sqlContext.read.json(value)

      //df.printSchema()
      df.rdd.map(record => {
        val apacheLogEvent = new ApacheLogEvents()
        apacheLogEvent.setClientId(record.getString(0))
        apacheLogEvent.setContentSize(record.getLong(1))
        apacheLogEvent.setIpv4(record.getString(2))
        apacheLogEvent.setReferrer(record.getString(3))
        apacheLogEvent.setRequestUri(record.getString(4))
        apacheLogEvent.setStatus(record.getLong(5).toInt)
        apacheLogEvent.setTimestamp(record.getString(6))
        apacheLogEvent.setUseragent(record.getString(7))
        apacheLogEvent
      })

    })


    import com.datastax.spark.connector.streaming._

    //Actual Analytics

    logLinesDStream.filter(_.getRequestUri.length() != 0)
                   .map(apacheLogEvent => {
                      val productUri = apacheLogEvent.getRequestUri.toString.split(" ")(1)
                      val product = productUri.split("/")(3)
                      (product, 1)
                    })
                   .reduceByKey(_ + _)
                   .saveToCassandra(cassandraKeySpace, "page_views", SomeColumns("page", "views"))



    logLinesDStream.map(apacheLogEvent => (apacheLogEvent.getStatus, 1))
                   .reduceByKey(_ + _)
                   .saveToCassandra(cassandraKeySpace, "status_counter", SomeColumns("status_code", "count"))



    logLinesDStream.filter(_.getIpv4.length() != 0)
                   .map(apacheLogEvent => {
                      val countryCodeMap =  countryCodeBrodcast.value
                      val octets = apacheLogEvent.getIpv4.toString.split("\\.")
                      val key = octets(0) + "." + octets(1)
                      val countryCode = countryCodeMap.getOrElse(key, "unknown country")
                      (countryCode, 1)
                    })
                   .reduceByKey(_ + _)
                   .saveToCassandra(cassandraKeySpace, "visits_by_country", SomeColumns("country", "count"))


    //Saving the offset after processing and saving data. So there could be duplicate data on restarting the streaming app
    kafkaStream.foreachRDD(rdd => {
      offsetStore.saveOffsets(topic, rdd)
    })


    ssc.start()
    ssc.awaitTermination()
  }
}
