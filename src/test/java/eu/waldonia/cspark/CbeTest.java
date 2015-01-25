package eu.waldonia.cspark;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static org.junit.Assert.*;

import java.io.Serializable;
import java.util.*;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.storage.StorageLevel;
import org.junit.*;

import scala.Tuple2;

import com.datastax.spark.connector.japi.CassandraRow;

// Note need to make serializable as the Function inner class has to be S and hence needs containing class to be
public class CbeTest extends AbstractCSparkTest implements Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = 2427600610912087082L;
    
    private static final String TEST_CBE = "cbe";
    private static final String ELIG_CLS = "eligcls";
    private static final String INSTRID = "instrid";
    private static final String ELIGCLS = "eligcls";
    private static final String ELIGHC = "elighc";
    private static final String ELIGREASON = "eligreason";
    private static final String ISIN = "isin";
    
    private JavaRDD<CassandraRow> tableRDD;
    
    @Before
    public void setUp() {
	tableRDD = javaFunctions(sc).cassandraTable(TEST_KEYSPACE, TEST_CBE);
	tableRDD.persist(StorageLevel.MEMORY_AND_DISK());	// we will reuse this
    }

    @Test
    public void testAccessMap() {
	tableRDD.map(
		new Function<CassandraRow, Map<String,List<String>>>() {

		    private static final long serialVersionUID = -8486237623245232984L;

		    private Map<String,List<String>> cbeInstr = new HashMap<String,List<String>>();
		    
		    @Override
		    public Map<String,List<String>> call(CassandraRow cassandraRow)
			    throws Exception {
			
			Map<Object,Object> eligCls = cassandraRow.getMap(ELIG_CLS);
			return cbeInstr;
		    }
		});
	
    
	assertNotNull(tableRDD.first());
    }
    
    
    /**
     * Turn the CassandraRow from the tableRDD (see setUp) 
     */
    @Test
    public void testToPairMap() {
	
	PairFunction<CassandraRow, String, List<Object>> keyByInstrId = 
		new PairFunction<CassandraRow, String, List<Object>>() {

		    @Override
		    public Tuple2<String, List<Object>> call(CassandraRow row)
			    throws Exception {

			String instrid = row.getString(INSTRID);
			Map<Object,Object> eligCls = row.getMap(ELIGCLS);
			Map<Object,Object> eligHc = row.getMap(ELIGHC);
			Map<Object,Object> eligReason = row.getMap(ELIGREASON);
			String isin = row.getString(ISIN);
			
			List<Object> data = new ArrayList<Object>();
			data.add(instrid);
			data.add(eligCls);
			data.add(eligHc);
			data.add(eligReason);
			data.add(isin);
			
			return new Tuple2<String,List<Object>>(instrid, data);
		    }
	    
	};
	
	JavaPairRDD<String, List<Object>> instrIdRows = tableRDD.mapToPair(keyByInstrId);
	
	assertNotNull(instrIdRows);
	assertTrue(instrIdRows.count() > 0);
	
    }
    
    
    @Test
    public void testKeyingByCB() {
	
	Function<String,List<String>> createInstrList = new Function<String,List<String>>() {

	    @Override
	    public List<String> call(String instrId) throws Exception {
		List<String> instrIds = new ArrayList<String>();
		instrIds.add(instrId);
		return instrIds;
	    }
	    
	};
	

	Function2<List<String>, String,List<String>> addInstrToList = new Function2<List<String>, String,List<String>>() {

	    @Override
	    public List<String> call(List<String> instrIds, String instrId)
		    throws Exception {
		instrIds.add(instrId);
		return instrIds;
	    }
	    
	};

	
	Function2<List<String>, List<String>, List<String>> combine = new Function2<List<String>, List<String>, List<String>>() {

	    @Override
	    public List<String> call(List<String> instrIds, List<String> instrIds2)
		    throws Exception {
		instrIds.addAll(instrIds2);
		return instrIds;
	    }
	    
	};

	
	PairFunction<CassandraRow, Map<String,String>, String> keyByEligibility = 
		new PairFunction<CassandraRow, Map<String,String>, String>() {

		    @Override
		    public Tuple2<Map<String,String>, String> call(CassandraRow row)
			    throws Exception {

			String instrid = row.getString(INSTRID);
			Map<Object,Object> eligCls = row.getMap(ELIGCLS);
			
			Map<String,String> eligibility = new HashMap<String,String>(eligCls.size());
			for (Object e : eligCls.keySet()) {
			    eligibility.put(e.toString(), eligCls.get(e).toString());
			}
			
			
			return new Tuple2<Map<String,String>, String>(eligibility,instrid);
		    }
	    
	};
	
	
	
	JavaPairRDD<Map<String,String>,String> eligibilityRows = tableRDD.mapToPair(keyByEligibility);
	
	assertNotNull(eligibilityRows);
	assertTrue(eligibilityRows.count() > 0);
	
	Map<String,String> ecbY = new HashMap<String,String>();
	ecbY.put("ECB", "Y");
	
	// check data values
	List<String> ecbYMatches = eligibilityRows.lookup(ecbY);
	System.out.println(ecbYMatches);
	
	// test multivalues for (ECB,Y)
	
	JavaPairRDD<Map<String,String>,Iterable<String>> cbInstrDenorm = eligibilityRows.groupByKey();
	List<Iterable<String>> newEcbYMatches = cbInstrDenorm.lookup(ecbY);
	System.out.println(newEcbYMatches);
	
	
    }
}

