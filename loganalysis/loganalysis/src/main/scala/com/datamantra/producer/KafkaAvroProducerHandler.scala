package com.datamantra.producer

import java.io.InputStream
import java.util.Properties


import com.datamantra.logs_analyzer.utils.{Handler, LazyLogging}
import com.datamantra.sparkstreaming.LogAnalyzerAvroKafkaStreaming._
import com.datamantra.utils.ApacheAccessLogCombined
import com.twitter.bijection.avro.GenericAvroCodecs
import loganalysis.avro.ApacheLogEvents
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericRecord, GenericData}

//import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig, ProducerRecord}

import scala.collection.mutable.ArrayBuffer


class KafkaAvroProducerHandler(val bootstrap: String, val topicName: String) extends Handler with LazyLogging {
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
  }
  catch { case _: Throwable => ()
  }

  def close() = {
    try {
      logger.debug("Attempting to close the avro producer stream to kafka")
      producer.close()
    } catch { case _: Throwable => () }
  }

  val avroProps = new Properties()
  avroProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  avroProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  avroProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer")
  avroProps.put("schema.registry.url", "http://localhost:8081")

  val avroGenericProducer = new KafkaProducer[String, GenericRecord](avroProps)

  val avroProducer = new KafkaProducer[String, ApacheLogEvents](avroProps)


  val input : InputStream = getClass.getResourceAsStream("/loganalysis.properties")



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


  def publishAvroGenericRecord(apacheRecord: ApacheAccessLogCombined): Unit = {

    try {
      val parser = new Schema.Parser();
      val schema = parser.parse(ApacheLogEvents.SCHEMA$.toString);
      val genericRecord = new GenericData.Record(schema);

      genericRecord.put("ipv4", apacheRecord.ipAddress)
      genericRecord.put("clientId", apacheRecord.clientId)
      genericRecord.put("timestamp", apacheRecord.dateTime)
      genericRecord.put("requestUri", apacheRecord.requestURI)
      genericRecord.put("status", apacheRecord.responseCode)
      genericRecord.put("contentSize", apacheRecord.contentSize)
      genericRecord.put("referrer", apacheRecord.referrer)
      genericRecord.put("useragent", apacheRecord.useragent)

      val producerRecord = new ProducerRecord[String, GenericRecord](topicName, genericRecord)
      println("Sending producer record to topic: " + topicName)
      avroGenericProducer.send(producerRecord)

    } catch {
      case e: Throwable => logger.error("Error:: {}", e)
    }

  }



  def publishAvroRecord(apacheRecord: ApacheAccessLogCombined): Unit = {

    val prop: Properties = new Properties
    prop.load(input)
    val topic = prop.getProperty("com.loganalysis.app.kafka.topic")
    val apacheLogEvent: ApacheLogEvents = new ApacheLogEvents
    try {
      apacheLogEvent.setIpv4(apacheRecord.ipAddress)
      apacheLogEvent.setClientId(apacheRecord.clientId)
      apacheLogEvent.setTimestamp(apacheRecord.dateTime)
      apacheLogEvent.setRequestUri(apacheRecord.requestURI)
      apacheLogEvent.setStatus(apacheRecord.responseCode)
      apacheLogEvent.setContentSize(apacheRecord.contentSize)
      apacheLogEvent.setReferrer(apacheRecord.referrer)
      apacheLogEvent.setUseragent(apacheRecord.useragent)
      avroProducer.send(new ProducerRecord[String, ApacheLogEvents]("apache-log-events", apacheLogEvent)).get
      System.out.println("Complete")
    }
    catch {
      case ex: Exception => {
        ex.printStackTrace(System.out)
      }
    } finally {
      //producer.close
    }

  }

}
