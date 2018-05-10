package com.datamantra.logs_analyzer.offsetsource


import java.io.InputStream
import java.util.Properties

import com.datamantra.logs_analyzer.offsetsource.stores.OffsetsStore
import com.datamantra.logs_analyzer.sparkstreaming.LogAnalyzerKafkaStreaming._
import com.datamantra.logs_analyzer.utils.LazyLogging
import kafka.message.MessageAndMetadata
import kafka.serializer.Decoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils

import scala.reflect.ClassTag

/**
 * Created by hduser on 13/4/17.
 */
object KafkaSource extends LazyLogging {

  // Kafka input stream
  def kafkaStream[K: ClassTag, V: ClassTag, KD <: Decoder[K] : ClassTag, VD <: Decoder[V] : ClassTag]
  (ssc: StreamingContext, kafkaParams: Map[String, String], offsetsStore: OffsetsStore, topic: String): InputDStream[(K, V)] = {

    var topicsSet = Set[String]()
    topicsSet += topic


    val storedOffsets = offsetsStore.readOffsets(topic)
    val kafkaStream = storedOffsets match {
      case None =>
        // start from the latest offsets
        KafkaUtils.createDirectStream[K, V, KD, VD](ssc, kafkaParams, topicsSet)
      case Some(fromOffsets) =>
        // start from previously saved offsets
        val messageHandler = (mmd: MessageAndMetadata[K, V]) => (mmd.key, mmd.message)
        KafkaUtils.createDirectStream[K, V, KD, VD, (K, V)](ssc, kafkaParams, fromOffsets, messageHandler)
    }

    // save the offsets
    kafkaStream.foreachRDD(rdd => offsetsStore.saveOffsets(topic, rdd))

    kafkaStream
  }

}
