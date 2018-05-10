package com.datamantra.sensordataprocessor.ingestion

import java.util.zip.ZipException

import com.datamantra.logs_analyzer.offsetsource.stores.ZooKeeperOffsetsStore
import com.datamantra.sensordataprocessor.msgprocessor.{seMsgFormat, vcdMsgFormat}
import com.datamantra.sensordataprocessor.utils._
import com.typesafe.config.ConfigFactory
import kafka.message.MessageAndMetadata
import kafka.serializer.DefaultDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

object SparkStreamStarter extends App with LazyLogging {

  override def main(args: Array[String]): Unit = {

    logger.info("Inside main program, setting variables now")
    val applicationConf = ConfigFactory.load
    val master = applicationConf.getString("spark.app.master")

    val applicationName = applicationConf.getString("spark.app.name")


    val kafkaParams: Map[String, String] = Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> applicationConf.getString("spark.kafka.bootstrap.servers"),
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> applicationConf.getString("spark.kafka.auto.offset.reset"),
      ConsumerConfig.GROUP_ID_CONFIG ->  applicationConf.getString("spark.kafka.group.id"),
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> applicationConf.getString("spark.kafka.key.deserializer.class.config"),
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> applicationConf.getString("spark.kafka.value.deserializer.class.config"),
      //"security.protocol" -> applicationConf.getString("spark.kafka.security.protocol"),
      //"sasl.kerberos.service.name" -> applicationConf.getString("spark.kafka.security.service.name"),
      "zookeeper.connect" -> applicationConf.getString("spark.zookeeper.quorum")
    )

    logger.info("Kafka parameters are : " +kafkaParams.toList)

    //val conf = new SparkConf().setMaster(master).setAppName(applicationName)
    val conf = new SparkConf().setAppName(applicationName).setMaster(master)
    conf.set("spark.streaming.concurrentJobs", "4")

    //Input stream Topics
    val vcdTopicSet: Set[String] =  { applicationConf.getStringList("spark.kafka.topics.vcd.topic.list").asScala.toSet }
    val seTopicSet: Set[String] =   { applicationConf.getStringList("spark.kafka.topics.se.topic.list").asScala.toSet  }
    val stfTopicSet: Set[String] =  { applicationConf.getStringList("spark.kafka.topics.stf.topic.list").asScala.toSet }

    val vcdTopic : String  = { applicationConf.getString("spark.kafka.topics.vcd.topic.str") }
    val seTopic : String   = { applicationConf.getString("spark.kafka.topics.se.topic.str") }
    val stfTopic : String  = { applicationConf.getString("spark.kafka.topics.stf.topic.str") }

    val batchDuration = {
      val duration = applicationConf.getLong("spark.kafka.batch.duration.duration")
      applicationConf.getString("spark.kafka.batch.duration.type").toLowerCase match {
        case "seconds" => Seconds(duration)
        case "milliseconds" => Milliseconds(duration)
        case "minutes" => Minutes(duration)
      }
    }

    val zkQuorum = applicationConf.getString("spark.zookeeper.quorum")
    val zkNode = applicationConf.getString("spark.zookeeper.znode.parent")

    logger.info("Initializing the streaming and the spark context")
    val ssc = new StreamingContext(conf, batchDuration)
    val sc = ssc.sparkContext

    /*
      VCD EVENT MESSAGE PROCESSING
     */

    /*
    val vcdZkNode = zkNode + "/" + vcdTopic
    val vcdOffsetStore = new ZooKeeperOffsetsStore(zkQuorum, vcdZkNode)
    val vcdStoredOffsets = vcdOffsetStore.readOffsets(vcdTopic)

    val vcdKafkaStream = vcdStoredOffsets match {
      case None =>
        logger.info("No previously stored offsets found, starting from the latest offset")
        KafkaUtils.createDirectStream[Array[Byte],Array[Byte],DefaultDecoder,DefaultDecoder](ssc, kafkaParams, vcdTopicSet)
      case Some(fromOffsets) =>
        logger.info("Stored offsets found, starting from the previously stored offsets")
        logger.info("Previously stored Offsets range is : " +fromOffsets)
        val messageHandler = (mmd: MessageAndMetadata[Array[Byte],Array[Byte]]) => (mmd.key, mmd.message)
        KafkaUtils.createDirectStream[Array[Byte],Array[Byte],DefaultDecoder,DefaultDecoder,(Array[Byte],Array[Byte])](ssc, kafkaParams, fromOffsets, messageHandler)
    }

    // save the offsets
    vcdKafkaStream.foreachRDD(rdd => {
      vcdOffsetStore.saveOffsets(vcdTopic, rdd)
    })
*/
   // val vcdKafkaStream = KafkaUtils.createDirectStream[Array[Byte],Array[Byte],DefaultDecoder,DefaultDecoder](ssc, kafkaParams, vcdTopic)

    val vcdKafkaStream = KafkaUtils.createDirectStream[Array[Byte],Array[Byte],DefaultDecoder,DefaultDecoder](ssc, kafkaParams, vcdTopicSet)

    logger.info("VCD Event : Getting the 2nd tuple from the stream to get only the data and ignore the offset")
    val vcdMsg = vcdKafkaStream.map(_._2)

    logger.info("VCD Message data is loaded to the variable vcdMsg")

    logger.info("VCD Event : Invoking GzipUtil package to decompress the .gz file for VCD")

    val vcdGzipUtil = new GzipUtil with Serializable

    val vcdHdfsPath = applicationConf.getString("spark.hdfs.vcd.data.path")

    vcdMsg.foreachRDD( (rdd, time) => {
      val logMgr = new LoggerManager with Serializable
      try {
        if (!rdd.isEmpty()) {

        val vcdMsgRDD = rdd.map(record =>
          vcdGzipUtil.decompress(record)).flatMap(x => x.split("\n")).cache()

        //if (!vcdMsgRDD.isEmpty()) {

          logger.info("VCD Event : Validation and Parsing of the file header")

          val md = new MetadataParse with Serializable

          val vcdHeader = vcdMsgRDD.first()
          val mdMap = md.parseMD(vcdHeader)
          val uuid = mdMap("uuid")
          val pv = mdMap("pv")
          val ssn = mdMap("ssn").toInt
          val tsn = mdMap("tsn").toInt
          val typ = mdMap("type")

          logger.info("VCD Event : MetaData{uuid=" + uuid + ", pv=" + pv + ", ssn=" + ssn + ", tsn=" + tsn + ", type=" + typ + "}")

          val transFunctions = new TransformationFunctions with Serializable
          val sigParse = new SignalValidate with Serializable

          logger.info("VCD Event : Currently processing the file : " +vcdMsgRDD)
          logger.info("VCD Event : Parsing the data and applying transformations")

          val VCDsiglistAccumumlator = sc.accumulator[List[vcdMsgFormat]](VCDCustListAccumulator.zero(Nil))(VCDCustListAccumulator)

          val vcdData_tmp = vcdMsgRDD.filter(row => row != vcdHeader)
            .foreachPartition(part => {

              var vcdsigList = new ListBuffer[vcdMsgFormat]()

              part.foreach(line => {
                logger.info("Performing validations and parsing of the signal line")

                try {
                  val sigArray = sigParse.validateSignal(typ, line)

                  val sigTs = sigArray(0)
                  val sigName = sigArray(1)
                  val sigCode = sigArray(2)

                  logger.info("VCD Event - Signal [name = " + sigName + ", value = + " + sigCode + ", Timestamp = " + sigTs + " ]")

                  val vcdClass = new vcdMsgFormat(uuid, pv, ssn, tsn, transFunctions.getTimestampFromEpochTime(sigTs.toLong), sigName
                    , transFunctions.getVCDSignalDesc(sigName), sigCode
                    , transFunctions.getVCDSignalValue(sigName, transFunctions.rtrim(sigCode.toString)), transFunctions.CREATE_DT)

                  logger.info("VCD Class contents is : " + vcdClass.toString)

                  vcdsigList.append(vcdClass)

                  logger.info("Signal list size is : " + vcdsigList.size)

                }
                catch {
                  case e: IllegalArgumentException =>
                    logMgr.getFormattedException(e, SparkStreamStarter.getClass.getName)
                    logger.error("VCD Event : Signal validation failed")

                  case e: NoSuchMethodException =>
                    logMgr.getFormattedException(e, SparkStreamStarter.getClass.getName)
                    logger.error("VCD Event : No such transformation exists")
                }
              }
              )
              VCDsiglistAccumumlator.+=(vcdsigList.toList)
            }
            )

          logger.info("Signal list contents are : " +VCDsiglistAccumumlator.value)

          val vcdData = sc.parallelize(VCDsiglistAccumumlator.value)

          //logger.info("VCD Event : RDD count is  : " +vcdData.count())

          logger.info("VCD Event : Initializing the sql Context")
          val sqlContext = new org.apache.spark.sql.SQLContext(sc)

          import sqlContext.implicits._

          logger.info("VCD Event : Converting the RDD to Data Frame")

          //vcdData.saveAsTextFile("/home/kafka/Documents/Telelog"  + "/" + time)
          val vcdDataDF = vcdData.toDF()

          //logger.info("VCD Event : Data Frame count is  : " +vcdDataDF.count())

          logger.info("VCD Event : Writing the data frame to parquet file on hdfs")

          try {
             transFunctions.writeToParquet(vcdDataDF,"/home/kafka/Documents/Telelog")
          }
          catch {
            case e: Exception =>
              logger.error("VCD Event : Error in wrtiting the dataframe to parquet file - " +e.getMessage)
          }
        }
        else {
          logger.info("VCD Event : RDD is empty, skipping it")
        }
      }
      catch {
        case e: ZipException =>
          logMgr.getFormattedException(e,SparkStreamStarter.getClass.getName)
          logger.error("VCD Event : Decompression of the input file failed")

        case e: MetaDataParseException =>
          logMgr.getFormattedException(e,SparkStreamStarter.getClass.getName)
          logger.error("VCD Event : Metadata parsing failed")

        case e: Exception =>
          logMgr.getFormattedException(e,SparkStreamStarter.getClass.getName)
          logger.error("VCD Event : Error in processing the input file")
      }
    }
    )


    /*
      SE EVENT MESSAGE PROCESSING
     */
    val seKafkaStream = KafkaUtils.createDirectStream[Array[Byte],Array[Byte],DefaultDecoder,DefaultDecoder](ssc, kafkaParams, seTopicSet)

    logger.info("SE Event : Getting the 2nd tuple from the stream to get only the data and ignore the offset")
    val seMsg = seKafkaStream.map(_._2)

    logger.info("SE Message data is loaded to the variable seMsg")

    logger.info("SE Event : Invoking GzipUtil package to decompress the .gz file for SE")

    val seGzipUtil = new GzipUtil with Serializable

    val seHdfsPath = applicationConf.getString("spark.hdfs.se.data.path")

    seMsg.foreachRDD( (rdd, time) => {
      val logMgr = new LoggerManager with Serializable
      try {
        val seMsgRDD = rdd.map(record =>
          seGzipUtil.decompress(record)).flatMap(x => x.split("\n"))

        if (!seMsgRDD.isEmpty()) {

          logger.info("SE Event : Validation and Parsing of the file header")

          val md = new MetadataParse with Serializable

          val seHeader = seMsgRDD.first()
          val mdMap = md.parseMD(seHeader)
          val uuid = mdMap("uuid")
          val pv = mdMap("pv")
          val ssn = mdMap("ssn").toInt
          val tsn = mdMap("tsn").toInt
          val typ = mdMap("type")

          logger.info("SE Event : MetaData{uuid=" + uuid + ", pv=" + pv + ", ssn=" + ssn + ", tsn=" + tsn + ", type=" + typ + "}")

          val transFunctions = new TransformationFunctions with Serializable
          val sigParse = new SignalValidate with Serializable

          logger.info("SE Event : Currently processing the file : " +seMsgRDD)
          logger.info("SE Event : Parsing the data and applying transformations")

          val SEsiglistAccumumlator = sc.accumulator[List[seMsgFormat]](SECustListAccumulator.zero(Nil))(SECustListAccumulator)

          val seData_tmp = seMsgRDD.filter(row => row != seHeader)
            .foreachPartition(part => {

              var sesigList = new ListBuffer[seMsgFormat]()

              part.foreach(line => {
                logger.info("Performing validations and parsing of the signal line")

                try {
                  val sigArray = sigParse.validateSignal(typ, line)

                  val sigTs = sigArray(0)
                  val eventID = sigArray(1)
                  val eventData = transFunctions.parseSEEventData(sigArray(2))

                  logger.info("SE Event - Signal [event ID = " +eventID + ", event Data = " +eventData + ", Timestamp = " +sigTs + " ]")

                  val seClass = new seMsgFormat(uuid,pv,ssn,tsn,transFunctions.getTimestampFromEpochTime(sigTs.toLong),eventID.toInt
                    ,transFunctions.SESignalMapping(eventID.toInt)(0)
                    ,transFunctions.SESignalMapping(eventID.toInt)(1),eventData,transFunctions.CREATE_DT)

                  logger.info("SE Class contents is : " + seClass.toString)

                  sesigList.append(seClass)

                  logger.info("Signal list size is : " + sesigList.size)

                }
                catch {
                  case e: IllegalArgumentException =>
                    logMgr.getFormattedException(e, SparkStreamStarter.getClass.getName)
                    logger.error("SE Event : Signal validation failed")

                  case e: NoSuchMethodException =>
                    logMgr.getFormattedException(e, SparkStreamStarter.getClass.getName)
                    logger.error("SE Event : No such transformation exists")
                }
              }
              )
              SEsiglistAccumumlator.+=(sesigList.toList)
            }
            )

          logger.info("Signal list contents are : " +SEsiglistAccumumlator.value)

          val seData = sc.parallelize(SEsiglistAccumumlator.value)

          //logger.info("SE Event : RDD count is  : " +seData.count())

          logger.info("SE Event : Initializing the sql Context")
          val sqlContext = new org.apache.spark.sql.SQLContext(sc)

          import sqlContext.implicits._

          logger.info("SE Event : Converting the RDD to Data Frame")
          val seDataDF = seData.toDF()

          //logger.info("SE Event : Data Frame count is  : " +seDataDF.count())

          logger.info("SE Event : Writing the data frame to parquet file on hdfs")

          try {
            transFunctions.writeToParquet(seDataDF,seHdfsPath)
          }
          catch {
            case e: Exception =>
              logger.error("SE Event : Error in wrtiting the dataframe to parquet file - " +e.getMessage)
          }
        }
        else {
          logger.info("SE Event : RDD is empty, skipping it")
        }
      }
      catch {
        case e: ZipException =>
          logMgr.getFormattedException(e,SparkStreamStarter.getClass.getName)
          logger.error("SE Event : Decompression of the input file failed")

        case e: MetaDataParseException =>
          logMgr.getFormattedException(e,SparkStreamStarter.getClass.getName)
          logger.error("SE Event : Metadata parsing failed")

        case e: Exception =>
          logMgr.getFormattedException(e,SparkStreamStarter.getClass.getName)
          logger.error("SE Event : Error in processing the input file")
      }
    }
    )


    /*
      STF EVENT MESSAGE PROCESSING
     */
    /*    val stfKafkaStream = KafkaUtils.createDirectStream[Array[Byte],Array[Byte],DefaultDecoder,DefaultDecoder](ssc, kafkaParams, stfTopic)

        logger.info("STF Event : Getting the 2nd tuple from the stream to get only the data and ignore the offset")
        val stfMsg = stfKafkaStream.map(_._2)

        logger.info("STF Message data is loaded to the variable stfMsg")

        logger.info("STF Event : Invoking GzipUtil package to decompress the .gz file")

        val stfGzipUtil = new GzipUtil with Serializable

        val stfHdfsPath = applicationConf.getString("spark.hdfs.stf.data.path")

        stfMsg.foreachRDD( rdd => {
          val logMgr = new LoggerManager with Serializable
          try {
            val stfMsgRDD = rdd.map(record => stfGzipUtil.decompress(record)).flatMap(x => x.split("\n"))

            if (!stfMsgRDD.isEmpty()) {

              logger.info("STF Event : Validation and Parsing of the file header")

              val md = new MetadataParse with Serializable

              val stfHeader = stfMsgRDD.first()

              val mdMap = md.parseMD(stfHeader)
              val uuid = mdMap("uuid")
              val pv = mdMap("pv")
              val ssn = mdMap("ssn").toInt
              val tsn = mdMap("tsn").toInt
              val typ = mdMap("type")

              logger.info("STF Event : MetaData{uuid=" + uuid + ", pv=" + pv + ", ssn=" + ssn + ", tsn=" + tsn + ", type=" + typ + "}")

              // Creating an object for the class TransformationFunctions

              val transFunctions = new TransformationFunctions with Serializable
              val sigParse = new SignalValidate with Serializable

              logger.info("STF Event : Parsing the data and applying transformations for STF file")

              val stfData = stfMsgRDD.filter(row => row != stfHeader)
                .map(line => {
                  val sigArray = sigParse.parseSignal(typ,line)
                  val sigTs = sigArray(0)
                  val sigName = sigArray(1)
                  val sigCode = sigArray(2)
                  stfMsgFormat(uuid,pv,ssn,tsn,transFunctions.getTimestampFromEpochTime(sigTs.toLong),sigName,transFunctions.getSignalDesc(sigName)
                    ,sigCode,transFunctions.getSignalValue(sigName,transFunctions.rtrim(sigCode.toString)),transFunctions.CREATE_DT)
                })

              logger.info("STF Event : Initializing the sql Context")
              val sqlContext = new org.apache.spark.sql.SQLContext(sc)

              import sqlContext.implicits._
              //val hqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

              logger.info("STF Event : Converting the RDD to Data Frame")
              val stfDataDF = stfData.toDF()

              stfDataDF.printSchema()
              stfDataDF.show()

              logger.info("STF Event : Data Frame count is  : " +stfDataDF.count())

              //vcdDataDF.registerTempTable("vcdDataTbl")

              logger.info("STF Event : Writing the data frame to parquet file on hdfs")

              try {
                transFunctions.writeToParquet(stfDataDF,stfHdfsPath)
              } catch {
                case e: Exception =>
                  logger.error("STF Event : Error in wrtiting the dataframe to parquet file - " +e.getMessage)
              }
            }
            else {
              logger.info("STF Event : RDD is empty, skipping it")
            }
          }
          catch {
            case e: Exception =>
              logMgr.getFormattedException(e,SparkStreamStarter.getClass.getName)
              logger.error("STF Event : Error in processing the input file")
          }
        }
        )*/

    ssc.start()
    ssc.awaitTermination()
  }
}
