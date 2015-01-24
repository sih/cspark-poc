package eu.waldonia.cspark;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapColumnTo;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;

import com.datastax.spark.connector.japi.CassandraJavaUtil;

public class SimpleCBAnalyser {

    // used to access the Spark Context
    private JavaSparkContext sc;

    public JavaSparkContext getContext() {
	if (null == sc) {
	    SparkConf conf = new SparkConf()
	    .setMaster("local")
	    .set("spark.cassandra.connection.host", "localhost")
            .setAppName("Counting Journeys")
            .set("spark.executor.memory", "1g");
	    sc = new JavaSparkContext(conf);
	}
	return sc;
    }

    public static void main(String[] args) {
	SimpleCBAnalyser a = new SimpleCBAnalyser();
	JavaSparkContext sc = a.getContext();
	
	JavaRDD<String> journeys = CassandraJavaUtil.javaFunctions(sc)
		.cassandraTable("traffic", "journeys", mapColumnTo(String.class))
		.select("k_location_id");
	
	int count = 0;
	List<String> locationIds = journeys.collect();
	for (String lid : locationIds) {
	    System.out.println(lid);
	    if (++count > 10) break;
	}
    }

}
