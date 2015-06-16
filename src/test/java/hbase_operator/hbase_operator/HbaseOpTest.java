package hbase_operator.hbase_operator;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.json.JSONObject;
import org.junit.Test;

public class HbaseOpTest {

	@Test
	public void testHbaseOp() {
		fail("Not yet implemented");
	}

	@Test
	public void testJson_put() {
		fail("Not yet implemented");
	}

	@Test
	public void testCreate() {
		fail("Not yet implemented");
	}

	@Test
	public void testClose() {
		fail("Not yet implemented");
	}

	@Test
	public void testGetMap4Json() {
		fail("Not yet implemented");
	}

	@Test
	public void testGetJSONList4Json() {
		fail("Not yet implemented");
	}

	@Test
	public void testGetKeyValueMap4Json() throws IOException {
		
		HbaseOp hbase = new HbaseOp();
		String data = 	"{\"Sensor_Value\" : {"
				+"		\"1\" : \"25\","
				+"		\"2\" : \"27\""
								+"}"
				+"	}";
		JSONObject obj = new JSONObject(data).getJSONObject("Sensor_Value");
		Map mapper = hbase.getKeyValueMap4Json(obj);
		
		String ans = (String) mapper.get("2");
		System.out.println(ans);
		assertEquals(ans, "25");
	}

	@Test
	public void testtestwhile() throws IOException {
		
		HbaseOp hbase = new HbaseOp();
		String data = 	"{\"Data_List\" : {"
+"		\"1\" : {"
+"			\"Timestamp\" : \"2015-04-20T00:00:00+08:00\","
+"			\"Sensor_Value\" : {"
	+"			\"1\" : \"23\","
		+"		\"2\" : \"25\""
		+"		}"
		+"	},"
	+"	\"2\" : {"
		+"	\"Timestamp\" : \"2015-04-20T01:00:00+08:00\","
		+"	\"Sensor_Value\" : {"
		+"		\"1\" : \"24.6\","
		+"		\"2\" : \"28\""
		+"	}"
		+"	},"
	+"	\"3\" : {"
	+"		\"Timestamp\" : \"2015-04-20T02:00:00+08:00\","
	+"		\"Sensor_Value\" : {"
		+"		\"1\" : \"25\","
		+"		\"2\" : \"27\""
						+"}"
		+"	}"
		+"	}}";
		JSONObject obj = new JSONObject(data).getJSONObject("Data_List");
		List<JSONObject> data_list = hbase.getJSONList4Json(obj);
		hbase.testWhile(data_list);
	}
}
