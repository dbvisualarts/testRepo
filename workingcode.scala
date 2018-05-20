import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ConstantInputDStream
import com.datastax.spark.connector.streaming.toStreamingContextFunctions
import com.datastax.spark.connector.toNamedColumnRef
import com.datastax.spark.connector._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._

import org.apache.spark.streaming.dstream.ConstantInputDStream


  /**
 * Reading from Cassandra using Spark Streaming
 */
object cassandraStream extends App {
  case class Raw(date:Int, region:String, eventtype:String, time:java.sql.Timestamp, eventid:String, data:Map[String, String])

  val spark = SparkSession.builder().appName("Spark_Streaming_Cassandra").master("local[*]").getOrCreate()

  spark.conf.set("spark.sql.shuffle.partitions", "2")
  spark.conf.set("spark.cassandra.connection.host", "127.0.0.1")

  val KEY_SPACE_NAME = "ks_ope"
  val TABLE_NAME = "rum_raw"

  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))
  val cassandraRDD = ssc.cassandraTable[Raw](KEY_SPACE_NAME, TABLE_NAME).select("date","region","eventtype","time","eventid","data")

  val dstream = new ConstantInputDStream(ssc, cassandraRDD)
  
  import spark.implicits._
  dstream.foreachRDD { rdd =>
    rdd.keyBy(row => (row.date,row.time,row.eventtype,row.data.get("ipcountrycode"),row.data.get("ipcontinent"))).map(x => x._1).toDF("date","time","eventtype","ipcountrycode","ipcontinent").show(false)
    
    
    println("Total Records cont in DB : " + rdd.count)
    
    println(rdd.collect.mkString("\n"))
  }
  ssc.start()
  ssc.awaitTermination()

}