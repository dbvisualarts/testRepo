package com.datamantra.sensordataprocessor.utils

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Calendar, TimeZone}

import org.apache.commons.lang.exception.ExceptionUtils
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by rharike on 9/1/2017.
  */
class LoggerManager {

  def current_timestamp = new Timestamp((new java.util.Date).getTime)

  def getFormattedException(ex:Exception,classname:String) {
    val logger: Logger = LoggerFactory.getLogger(getClass.getName)
    logger.error(ExceptionUtils.getStackTrace(ex))

    var formattedException = new StringBuilder()
    formattedException.append(current_timestamp + ": " + classname + " failed with following exception.\n")
    formattedException.append("Message: " + ex.getMessage() + "\n")
    formattedException.append("Stack Trace: " + ExceptionUtils.getStackTrace(ex))

    logger.error(formattedException.toString())
  }

  def getCurrentUTCDate() : String = {
    val curDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
    val timeZone = TimeZone.getTimeZone("UTC")
    val curdatetmp = Calendar.getInstance(timeZone)

    val curDate = curDateFormat.format(curdatetmp.getTime())
    curDate
  }
}
