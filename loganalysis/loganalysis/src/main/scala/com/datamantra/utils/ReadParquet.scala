package com.datamantra.utils

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Created by kafka on 24/10/17.
 */
object ReadParquet {

  def main(args: Array[String]) {

    val sc = new SparkContext("local", "read parquet")

    val sqlContext = new SQLContext(sc)

    val df = sqlContext.read.load("/home/kafka/Documents/Telelog")
    df.printSchema()
    df.rdd.saveAsTextFile("/home/kafka/Documents/Telelog1")
  }
}
