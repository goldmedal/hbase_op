package hbase_operator.hbase_operator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.json.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class DCR_Handler {
	
	private static HbaseOp hBase;
	
	public static void main(String[] args) throws IOException  {
		
		
		String data = "{"

				+"	\"CPA_Info\" : {"
		+"\"Name\" : \"CPA01\","
		+"\"Company\" : \"FATEK\","
	+"	\"Timestamp\" : \"2015-04-21T12:00:00+08:00\""
	+"	},"

	+"	\"Sensor_Info\" : {"
+"		\"Sensor1\" : {"
+"			\"Type\" : \"Temperature\","
+"			\"FiledNumber\" : \"1\""		
+"		},"

+"	\"Sensor2\" : {"
+"			\"Type\" : \"Temperature\","
+"			\"FiledNumber\" : \"2\""
+"		}"

+"	},"

	+"\"Data_List\" : {"
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
		+"	}"

+"}";
		// String data = args[0];
		JSONObject obj = new JSONObject(data);
		String tableName = obj.getJSONObject("CPA_Info").getString("Name");
	//	System.out.print(tableName);
		
		hBase = new HbaseOp(tableName);
		hBase.json_put(data);
		hBase.close();
		
		
	}

}
