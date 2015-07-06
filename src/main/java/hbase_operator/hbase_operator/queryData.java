package hbase_operator.hbase_operator;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
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
    	
		String data = "";
		FileReader fr = new FileReader("/home/hduser/tomcat_workspace/input/query.json");
		BufferedReader br = new BufferedReader(fr);
		while (br .ready()) {
			data = data + br.readLine();
		}
		// System.out.println(data);
		// String data = args[0];
		JSONObject obj = new JSONObject(data);
		String tableName = obj.getJSONObject("CPA_Info").getString("Name");
	//	System.out.print(tableName);
		
		hBase = new HbaseOp(tableName);
		String returnValue = hBase.json_query(data);
		System.out.println(returnValue);
		hBase.close();
		
    	FileWriter fw = new FileWriter("/home/hduser/tomcat_workspace/output/query_result.json");
    	fw.write(returnValue);
    	fw.flush();
    	fw.close();
	    
	  }	
	
}
