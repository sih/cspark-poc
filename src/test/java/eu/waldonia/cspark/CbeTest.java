package eu.waldonia.cspark;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.*;
import static org.junit.Assert.*;

import java.io.Serializable;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.junit.Test;

import com.datastax.spark.connector.japi.*;

// Note need to make serializable as the Function inner class has to be S and hence needs containing class to be
public class CbeTest extends AbstractCSparkTest implements Serializable {

    private static final String TEST_CBE = "cbe";

    @Test
    public void testAccessMap() {
	JavaRDD<String> tableRDD = javaFunctions(sc).cassandraTable(
		TEST_KEYSPACE, TEST_CBE).map(
		new Function<CassandraRow, String>() {
		    /**
		     * 
		     */
		    private static final long serialVersionUID = -8486237623245232984L;

		    @Override
		    public String call(CassandraRow cassandraRow)
			    throws Exception {
			return cassandraRow.toString();
		    }
		});

	System.out.println(tableRDD.first());
    }
}
