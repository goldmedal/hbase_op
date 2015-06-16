package hbase_operator.hbase_operator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.JSONObject;


public class queryData {

	private static HbaseOp hBase;
	
    public static void main( String[] args ) throws IOException
    {
    		
		System.out.println("-------Start Query!-------");
		String data = "{"

				+"	\"CPA_Info\" : {"
		+"\"Name\" : \"CPA01\","
		+"\"Company\" : \"FATEK\","
	+"	\"Timestamp\" : \"2015-04-21T12:00:00+08:00\""
	+"	},"

+"\"Query_Request\" : {"
+"	\"Name\" : \"GetDataCountBySensor\","
	+"	\"Parameter\" : {"
	+"		\"Sensor_Name\" : \"Sensor2\","
	+"		\"Start\" : \"27\","
	+"		\"End\" : \"28\""
	+"	}"
	+"}"
+"}";
		
		// System.out.println(data);
		// String data = args[0];
		JSONObject obj = new JSONObject(data);
		String tableName = obj.getJSONObject("CPA_Info").getString("Name");
	//	System.out.print(tableName);
		
		hBase = new HbaseOp(tableName);
		String returnValue = hBase.json_query(data);
		System.out.println(returnValue);
		hBase.close();
	    
	  }	
	
}
