package com.datamantra.logs_analyzer.producer

import java.util.Properties


import com.datamantra.logs_analyzer.utils.{Handler, LazyLogging}

//import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer, Producer, ProducerConfig}

import scala.collection.mutable.ArrayBuffer


class KafkaProducerHandler(val bootstrap: String, val topicName: String) extends Handler with LazyLogging {
/*
  private val props = new Properties()
  props.put("serializer.class", "kafka.serializer.StringEncoder")
  props.put("metadata.broker.list", brokerList) // bootstrap.servers
  //props.put("bootstrap.servers", brokerList)
  private val config = new ProducerConfig(props)
*/
  private var producer: Producer[String, String] = null
  val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")


  try {
    if (producer == null) {
      logger.debug("Attempting to make connection with kafka")
      producer = new KafkaProducer[String, String](props)
    } else {
      logger.debug("Reusing the kafka connection")
    }
  } catch { case _: Throwable => () }

  def close() = {
    try {
      logger.debug("Attempting to close the producer stream to kafka")
      producer.close()
    } catch { case _: Throwable => () }
  }

  def send(producerRecord: ProducerRecord[String, String]) = {
    try {
      // logger.debug("Attempting to send the key to kafka broker")
      producer.send(producerRecord)
    }
  }

  def publish(record: String) = {
    try {
      val producerRecord = new ProducerRecord[String, String](topicName, java.util.UUID.randomUUID().toString, record)
      //send(KeyedMessage[String, String](topicName, java.util.UUID.randomUUID().toString, null,record))
      send(producerRecord)
    } catch {
      case e: Throwable => logger.error("Error:: {}", e)
    }
  }

  def publishBuffered(records: ArrayBuffer[String]) = {
    try {
      records.foreach { record => {
        val producerRecord = new ProducerRecord[String, String](topicName, java.util.UUID.randomUUID().toString, record)
        send(producerRecord)
        //send(KeyedMessage[String, String](topicName, java.util.UUID.randomUUID().toString, null, record))
      }
      }
    } catch {
      case e: Throwable => logger.error("Error:: {}", e)
    }
  }
}
