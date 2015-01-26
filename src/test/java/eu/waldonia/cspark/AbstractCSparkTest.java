package eu.waldonia.cspark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.BeforeClass;

public abstract class AbstractCSparkTest {
    
    // used to access the Spark Context
    protected static JavaSparkContext sc;
    
    protected static final String TEST_KEYSPACE = "spark_15";
    
    @BeforeClass
    public static void init() {

	SparkConf conf = new SparkConf().setMaster("local")
		.set("spark.cassandra.connection.host", "localhost")
		.setAppName("CBE")
		.set("spark.executor.memory", "1g");
	sc = new JavaSparkContext(conf);

    }

}
