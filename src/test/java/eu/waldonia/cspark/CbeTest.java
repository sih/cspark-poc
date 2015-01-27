package eu.waldonia.cspark;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static org.junit.Assert.*;

import java.io.Serializable;
import java.util.*;
import java.util.Map.Entry;

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
    private JavaPairRDD<String, CassandraRow> rowsKeyedByInstrid;
    private JavaRDD<Tuple2<Tuple2<String, String>, String>> instridsKeyedByCBClsf;

    @Before
    public void setUp() {
	tableRDD = javaFunctions(sc).cassandraTable(TEST_KEYSPACE, TEST_CBE);
	tableRDD.persist(StorageLevel.MEMORY_AND_DISK()); // we will reuse this
    }

    @Test
    public void testNormalizeTableByInstrid() {
	PairFunction<CassandraRow, String, CassandraRow> keyRowByInstrid = new PairFunction<CassandraRow, String, CassandraRow>() {

	    @Override
	    public Tuple2<String, CassandraRow> call(CassandraRow row)
		    throws Exception {

		String instrid = row.getString(INSTRID);
		return new Tuple2(instrid, row);
	    }

	};

	rowsKeyedByInstrid = tableRDD.mapToPair(keyRowByInstrid);

	assertNotNull(rowsKeyedByInstrid);

	// check the data
	List<CassandraRow> row1234 = rowsKeyedByInstrid.lookup("1234");
	assertNotNull(row1234);
	assertEquals(1, row1234.size());
	List<CassandraRow> row1239 = rowsKeyedByInstrid.lookup("1234");
	assertNotNull(row1239);
	assertEquals(1, row1239.size());

	rowsKeyedByInstrid.persist(StorageLevel.MEMORY_AND_DISK());
    }

    
    @Test
    public void testNormalizeTableByInstrId() {
	
	Function<CassandraRow, Tuple2<String,List<Tuple2<String,String>>>> cbClsFlattener = 
		new Function<CassandraRow, Tuple2<String,List<Tuple2<String,String>>>>() {
		    
		    @Override
		    public Tuple2<String, List<Tuple2<String,String>>> call(CassandraRow row)
			    throws Exception {

			String instrId = row.getString(INSTRID);
			Map<Object,Object> eligcls = row.getMap(ELIG_CLS);
			List<Tuple2<String,String>> clssfs = new ArrayList<Tuple2<String,String>>();
			for (Object e : eligcls.keySet()) {
			    Tuple2<String,String> cbcls = new Tuple2<String,String>(e.toString(),eligcls.get(e).toString());
			    clssfs.add(cbcls);
			}
			
			return new Tuple2<String,List<Tuple2<String,String>>>(instrId, clssfs);
		    }
		};
		
		
	JavaRDD<Tuple2<String,List<Tuple2<String,String>>>> flattenedCbCls = tableRDD.map(cbClsFlattener);
	
	List<Tuple2<String,List<Tuple2<String,String>>>> instrs = flattenedCbCls.collect();
	for (Tuple2<String, List<Tuple2<String, String>>> t : instrs) {
	    
	    if (t._1.equals("1234")) {
		assertEquals(4,t._2.size());
		assertTrue(t._2.contains(new Tuple2<String,String>("BOE","Y")));
		assertTrue(t._2.contains(new Tuple2<String,String>("SNB","N")));
		assertTrue(t._2.contains(new Tuple2<String,String>("ECB","Y")));
		assertTrue(t._2.contains(new Tuple2<String,String>("BOC","Y")));
	    }
	    else if (t._1.equals("1235")) {
		assertEquals(2,t._2.size());
		assertTrue(t._2.contains(new Tuple2<String,String>("BOE","Y")));
		assertTrue(t._2.contains(new Tuple2<String,String>("BOC","Y")));
	    }
	    else if (t._1.equals("1236")) {
		assertEquals(1,t._2.size());
		assertTrue(t._2.contains(new Tuple2<String,String>("SNB","Y")));
	    }
	    else if (t._1.equals("1237")) {
		assertEquals(2,t._2.size());
		assertTrue(t._2.contains(new Tuple2<String,String>("SNB","Y")));
		assertTrue(t._2.contains(new Tuple2<String,String>("BOC","Y")));
	    }
	    else if (t._1.equals("1239")) {
		assertEquals(1,t._2.size());
		assertTrue(t._2.contains(new Tuple2<String,String>("ECB","Y")));
	    }
	    else {
		fail("Unexpected item in bagging area");
	    }

	}
	
    }
    
    
    @Test
    public void testNormalizeTableByCB() {
	FlatMapFunction<CassandraRow, Tuple2<String, String>> flattenByCBCode = new FlatMapFunction<CassandraRow, Tuple2<String, String>>() {

	    @Override
	    public Iterable<Tuple2<String, String>> call(CassandraRow row)
		    throws Exception {

		List<Tuple2<String, String>> cbCodes = new ArrayList<Tuple2<String, String>>();

		Map<Object, Object> eligCls = row.getMap(ELIG_CLS);
		for (Object ec : eligCls.keySet()) {
		    Tuple2 cls = new Tuple2(ec.toString(), eligCls.get(ec)
			    .toString());
		    cbCodes.add(cls);
		}

		return cbCodes;
	    }

	};

	JavaRDD<Tuple2<String, String>> clssfs = tableRDD
		.flatMap(flattenByCBCode);

	assertNotNull(clssfs);
	assertTrue(clssfs.count() > 0);
/*
	Tuple2 ecbY = new Tuple2("ECB", "Y");
	List<Tuple2<String, String>> results = clssfs.toArray();
	for (Tuple2<String, String> tuple2 : results) {
	    System.out.println("(" + tuple2._1 + "," + tuple2._2 + ")");
	}
*/	
    }

    @Test
    public void testNormalizeTableByCBAndInstrId() {
	
	FlatMapFunction<CassandraRow, Tuple2<Tuple2<String, String>, String>> flattenByCBCodeAndInstr = new FlatMapFunction<CassandraRow, Tuple2<Tuple2<String, String>, String>>() {

	    
	    /*
	     * Need the Map below to pull out the multivalues from the C* map field
	     */
	    @Override
	    public Iterable<Tuple2<Tuple2<String, String>, String>> call(
		    CassandraRow row) throws Exception {

		List<Tuple2<Tuple2<String, String>, String>> cbCodes = new ArrayList<Tuple2<Tuple2<String, String>, String>>();

		Map<Object, Object> eligCls = row.getMap(ELIG_CLS);
		String instrid = row.getString(INSTRID);
		for (Object ec : eligCls.keySet()) {
		    Tuple2 cls = new Tuple2(ec.toString(), eligCls.get(ec).toString());
		    Tuple2<Tuple2<String, String>, String> instrIdByCbCls = new Tuple2<Tuple2<String, String>, String>(cls, instrid);
		    cbCodes.add(instrIdByCbCls);
		}

		return cbCodes;
	    }

	};

	/*
	 * Produces:
	 * (BOE,Y) => 1234
	 * (SNB,N) => 1234
	 * (ECB,Y) => 1234
	 * (BOC,Y) => 1234
	 * (ECB,Y) => 1239
	 * (SNB,Y) => 1237
	 * (BOC,Y) => 1237
	 * (BOE,Y) => 1235
	 * (BOC,Y) => 1235
	 * (SNB,Y) => 1236
	 */
	instridsKeyedByCBClsf = tableRDD.flatMap(flattenByCBCodeAndInstr);

	assertNotNull(instridsKeyedByCBClsf);
	assertTrue(instridsKeyedByCBClsf.count() > 0);

	String expectedMultiInstrid = "1234";
	Tuple2<String,String> expectedMultiClsf = new Tuple2<String,String>("BOC","Y");	
	
	int instrCount = 0;
	int clsfCount = 0;
	
	List<Tuple2<Tuple2<String, String>, String>> results = instridsKeyedByCBClsf.collect(); // ONLY FOR TESTING
	for (Tuple2<Tuple2<String, String>, String> item : results) {
	    Tuple2<String,String> clsf = item._1;
	    String instrid = item._2;
	    if (expectedMultiInstrid.equals(instrid)) instrCount++;
	    if (expectedMultiClsf.equals(clsf)) clsfCount++;
	}
	
	// check multirows at present
	assertTrue(instrCount > 1);
	assertTrue(clsfCount > 1);

	// now group by the tuple (cb, clsf)
	PairFunction<Tuple2<Tuple2<String,String>,String>, Tuple2<String,String>, String> pf
	 = new PairFunction<Tuple2<Tuple2<String,String>,String>, Tuple2<String,String>, String>() {
	    
	    @Override
	    public Tuple2<Tuple2<String, String>, String> call(Tuple2<Tuple2<String, String>, String> input) throws Exception {
		return new Tuple2(input._1,input._2);
	    }
	};
	
	// use the function to collect the keys as keys (i.e. pull them from the map)
	JavaPairRDD<Tuple2<String,String>, String> normalizedInstrsKeyedByCBClsf = 
		instridsKeyedByCBClsf.mapToPair(pf);

	// now group by the key
	JavaPairRDD<Tuple2<String,String>, Iterable<String>> denormalizedInstrsKeyedByCBClsf = 
		normalizedInstrsKeyedByCBClsf.groupByKey();
	
	Tuple2<String,String> bocY = new Tuple2("BOC","Y");
	Iterable<Iterable<String>> bocInstrs = denormalizedInstrsKeyedByCBClsf.lookup(bocY);

	int countBocYItems = 0;
	for (Iterable<String> i : bocInstrs) {
	    for (String s : i) {
		countBocYItems++;
		assertTrue(checkBOC(s));
	    }
	}
	
	assertEquals(3,countBocYItems);
	
    }

    /*
	 * (BOE,Y) => 1234
	 * (SNB,N) => 1234
	 * (ECB,Y) => 1234
	 * (BOC,Y) => 1234
	 * (ECB,Y) => 1239
	 * (SNB,Y) => 1237
	 * (BOC,Y) => 1237
	 * (BOE,Y) => 1235
	 * (BOC,Y) => 1235
	 * (SNB,Y) => 1236
     */
    private boolean checkBOC(String s) {
	return s.equals("1234") || s.equals("1235") || s.equals("1237");
    }

    @Test
    public void testNormalizeTableByCBAndInstrIdUsingPairMap() {
	
	// TODO - not sure this is a goer	
    }

}
