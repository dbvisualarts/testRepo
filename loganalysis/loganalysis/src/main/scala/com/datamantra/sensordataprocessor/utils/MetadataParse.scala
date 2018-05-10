package com.datamantra.sensordataprocessor.utils

import scala.collection.mutable.{Map => MutableMap}

/**
  * Created by rharike on 8/31/2017.
  */

class MetadataParse extends LazyLogging {
  val logMgr = new LoggerManager

  def parseMD(mdLine : String) : MutableMap[String,String] = {
    logger.info("Inside parseMD method")

    if(mdLine == null || mdLine.length == 0) {
      logger.error("Metadata of the file must not be null, skipping the file")
      logger.error("Metadata line is : " +mdLine)
      throw MetaDataParseException("Metadata of the file must not be null")
    }

    val mdDataArray = mdLine.replace("#","").trim.split(",")
    var mdItems = MutableMap[String,String]()

    for (kv <- mdDataArray) {
      val keyElem = kv.split("=")(0)
      val valueElem = kv.split("=")(1)
      mdItems(keyElem) = valueElem
    }

    if (mdItems.contains("uuid") && mdItems.contains("pv") && mdItems.contains("ssn") && mdItems.contains("tsn")
      && mdItems.contains("type")) {
      logger.info("Metadata has all the attributes")
    }
    else {
      logger.error("The meta data line does not contain all necessary attributes, hence skipping the file")
      logger.error("Metadata line is : " +mdLine)
      throw MetaDataParseException("The meta data line does not contain all necessary attributes")
    }
    mdItems
  }
}
