package eu.waldonia.cspark;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapColumnTo;
import static org.junit.Assert.*;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.junit.*;

import com.datastax.spark.connector.japi.CassandraJavaUtil;

public class PlayersTest extends AbstractCSparkTest {

    private static final String TEST_PLAYERS = "players";

    @Test
    public void test() {
	JavaRDD<String> players = CassandraJavaUtil.javaFunctions(sc)
		.cassandraTable(TEST_KEYSPACE, TEST_PLAYERS, mapColumnTo(String.class))
		.select("player");
	
	List<String> name = players.collect();
	assertFalse(name.isEmpty());
	
	
    }

}
