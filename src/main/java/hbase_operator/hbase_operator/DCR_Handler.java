package hbase_operator.hbase_operator;

import java.io.FileReader;
import java.io.BufferedReader;

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
		
		String data = "";
		FileReader fr = new FileReader("/home/hduser/tomcat_workspace/input/dcr.json");
		BufferedReader br = new BufferedReader(fr);
		while (br .ready()) {
			data = data + br.readLine();
		}
		System.out.println("-------------data-----------");
		System.out.println(data);
		// String data = args[0];
		JSONObject obj = new JSONObject(data);
		String tableName = obj.getJSONObject("CPA_Info").getString("Name");
	//	System.out.print(tableName);
		
		hBase = new HbaseOp(tableName);
		hBase.json_put(data);
		hBase.close();
		
		
	}

}
