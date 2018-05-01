import java.util.HashMap;
import java.util.HashSet;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import org.apache.spark.sql.cassandra.*;

import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.cql.CassandraConnectorConf;
import com.datastax.spark.connector.rdd.ReadConf;
import com.datastax.driver.core.Session;
import com.datastax.spark.connector.*;
import org.apache.spark.sql.catalyst.*;
public class CassandraWithSpark {

	public static void main(String[] args) throws Exception {
	   
//	    String brokers = "127.0.0.1:9092";


	    // Create context with a 30 seconds batch interval
//	    SparkConf sparkConf = new SparkConf().setAppName("javadf").set("spark.cassandra.connection.host", "127.0.0.1");
//	    SparkContext sc = new SparkContext("spark://127.0.0.1:7077", "test", sparkConf);
//	    SparkContext sc = new SparkContext(sparkConf);
		SparkSession spark = SparkSession.builder().config("spark.cassandra.connection.host", "127.0.0.1").appName("cassandara exmple").getOrCreate();
//	    SparkSession spark = SparkSession.builder().appName("cassandara exmple").getOrCreate();
	    Dataset<Row> empRecords = spark.read().format("org.apache.spark.sql.cassandra").option("keyspace","test").option("table", "kv").load();
	    empRecords.show();
	    empRecords.write().format("org.apache.spark.sql.cassandra").option("keyspace","test").option("table", "kv1").mode(SaveMode.Ignore).save();
	    
//	    CassandraConnector connector = CassandraConnector.apply(sc.getConf());
//	    
//	    try (Session session = connector.openSession()) {
//	    	session.execute("DROP KEYSPACE IF EXISTS java_api");
//	    	session.execute("CREATE KEYSPACE java_api WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
//	        session.execute("CREATE TABLE java_api.products (id INT PRIMARY KEY, name TEXT, parents LIST<INT>)");
//	        session.execute("CREATE TABLE java_api.sales (id UUID PRIMARY KEY, product INT, price DECIMAL)");
//	        session.execute("CREATE TABLE java_api.summaries (product INT PRIMARY KEY, summary DECIMAL)");
//	    }
	    
	}
	
}
