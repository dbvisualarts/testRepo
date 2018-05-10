package com.datamantra.logs_analyzer.utils

import java.text.SimpleDateFormat
import java.util.Date
import com.datamantra.utils.{ApacheAccessLog, ViewedProduts}
import org.apache.spark.broadcast.Broadcast

import scala.collection.mutable

/**
 * Created by pramod on 26/12/15.
 */
object AccessLogUtils {

  def getHour(dateTime: String): Int = {

    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val d: Date = sdf.parse(dateTime)
    d.getHours
  }

  def getMinPattern(dateTime: String): String = {

    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val ddf = new SimpleDateFormat("yyyy-MM-dd HH:mm")
    ddf.format(sdf.parse(dateTime))
  }



  def toDate(sourceDate: String, srcPattern: String, destPattern: String): String = {
    val srcdf = new SimpleDateFormat(srcPattern)
    val destdf = new SimpleDateFormat(destPattern)
    val d = srcdf.parse(sourceDate)
    val result = destdf.format(d)
    result
  }

  def geProducts(requestURI: String): String = {
    val parts = requestURI.split("/")
    parts(3)
  }


  def createCountryCodeMap(filename: String): mutable.Map[String, String] = {

    val cntryCodeMap = mutable.Map[String, String]()
    val lines = scala.io.Source.fromFile(filename).getLines()
    while (lines.hasNext) {
      val Array(ipOctets, cntryCode) = lines.next().split(" ")
      //for(line <- lines) {
      //val values = line.split(",")
      cntryCodeMap.put(ipOctets, cntryCode)
    }
    cntryCodeMap
  }


  def getCountryCode(ipaddress: String, cntryCodeBroadcast: Broadcast[mutable.Map[String, String]]): String = {
    val octets = ipaddress.split("\\.")
    val key = octets(0) + "." + octets(1)

    //cntryCodeBroadcast.value(classABIP)
    val cntryMap = cntryCodeBroadcast.value
    cntryMap.getOrElse(key, "unknown country")
  }

  def getViewedProducts(access_log: ApacheAccessLog, cntryCodeBroadcast: Broadcast[mutable.Map[String, String]]): ViewedProduts = {

      ViewedProduts(getCountryCode(access_log.ipAddress, cntryCodeBroadcast),
      access_log.userId,
      toDate(access_log.dateTime,"dd/MMM/yyyy:HH:mm:ss Z", "yyyy-MM-dd HH:mm:ss"),
      geProducts(access_log.requestURI),
      access_log.responseCode,
      access_log.contentSize)
  }
}