package com.datamantra.sensordataprocessor.producer;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;






public class BasicProducerExample
{
  public BasicProducerExample() {}
  
  public static void main(String[] paramArrayOfString)
    throws IOException
  {
    Properties localProperties = new Properties();
    localProperties.put("bootstrap.servers", "localhost:9092");
    

    //localProperties.put("security.protocol", "PLAINTEXTSASL");
    
    localProperties.put("acks", "all");
    localProperties.put("retries", Integer.valueOf(0));
    localProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    localProperties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    
    KafkaProducer localKafkaProducer = new KafkaProducer(localProperties);
    //BasicProducerExample.TestCallback localTestCallback = new BasicProducerExample.TestCallback(null);
    

    byte[] arrayOfByte = Files.readAllBytes(new File(paramArrayOfString[1]).toPath());
    

    ProducerRecord localProducerRecord = new ProducerRecord(paramArrayOfString[0], arrayOfByte);
    localKafkaProducer.send(localProducerRecord);
    

    localKafkaProducer.close();
  }
}
