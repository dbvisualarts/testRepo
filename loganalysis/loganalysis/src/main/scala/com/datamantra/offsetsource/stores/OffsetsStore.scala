package com.datamantra.logs_analyzer.offsetsource.stores

import kafka.common.TopicAndPartition
import org.apache.spark.rdd.RDD

/**
 * Created by hduser on 13/4/17.
 */
trait OffsetsStore {

  def readOffsets(topic: String): Option[Map[TopicAndPartition, Long]]

  def saveOffsets(topic: String, rdd: RDD[_]): Unit

}
