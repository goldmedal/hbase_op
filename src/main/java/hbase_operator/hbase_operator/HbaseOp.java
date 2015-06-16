package hbase_operator.hbase_operator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.json.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;

import static org.apache.hadoop.hbase.CellUtil.cloneFamily;
import static org.apache.hadoop.hbase.CellUtil.cloneQualifier;
import static org.apache.hadoop.hbase.CellUtil.cloneValue;
import static org.apache.hadoop.hbase.CellUtil.cloneRow;

import java.util.HashSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.ArrayList;

public class HbaseOp 
{
	
	private static Configuration conf;
	private static Connection connection;
	private static Admin admin;
	private static TableName table;
    
	public HbaseOp( String tableName )  throws IOException {
		
    	HbaseOp.conf = HBaseConfiguration.create();
    	HbaseOp.connection =  ConnectionFactory.createConnection(conf);
    	HbaseOp.admin = connection.getAdmin();
    	
        HbaseOp.table = TableName.valueOf(tableName);
        
        if(!(HbaseOp.admin.tableExists(table))) {
        	this.create(tableName);;
        }
   
		
	}
	
	public HbaseOp( )  throws IOException {
		
 
	}
	
    public void json_put( String jsonData ) throws IOException
    {
    	 
    	//  json parser
		JSONObject obj = new JSONObject(jsonData);
		
		// sensor_info to Map
		JSONObject sensor_info = obj.getJSONObject("Sensor_Info");
	    Map sensorNumNameMap = getNumNameMap4Json(sensor_info);
	    Map sensorNameTypeMap = getNameTypeMap4Json(sensor_info);
	    
	    // data_list to list

	    JSONObject data = obj.getJSONObject("Data_List");
	    List<JSONObject> data_list = getJSONList4Json(data);
	    
	    
	    Table t = this.connection.getTable(table);	    
	   
	    List<Put> lp = new ArrayList<Put>();
	    
	    Iterator<JSONObject> i_it = data_list.iterator();

	    while(i_it.hasNext()) {
	    	
	    	JSONObject jobj = i_it.next();
	    	JSONObject sensor_value = jobj.getJSONObject("Sensor_Value");
	    	Map value_list = getKeyValueMap4Json(sensor_value);
	    	    	
	    	for (Object key : value_list.keySet()){
			    String col = (String) sensorNumNameMap.get(key);
			    String value = (String) value_list.get(key);
			    String time = (String) jobj.getString("Timestamp");
			    String num = (String) key;
			    String type = (String) sensorNameTypeMap.get(col);
			    
			    Put p = new Put(Bytes.toBytes(time));
			    p.addColumn(Bytes.toBytes("FIELD"), Bytes.toBytes(col), Bytes.toBytes(value));
			    p.addColumn(Bytes.toBytes("TYPE"), Bytes.toBytes(col), Bytes.toBytes(type));
			    p.addColumn(Bytes.toBytes("FIELD_NUM"), Bytes.toBytes(col), Bytes.toBytes(num));
			    
			    lp.add(p);
	    	} 
	    }
	    
	    t.put(lp);
	    t.close();

	    
    }
    
    public String json_query( String jsonData ) throws IOException {
    
    	String Ack = "";
    	String Message = "";
    	String result = "";
    	JSONObject obj = new JSONObject(jsonData);
    	
    	JSONObject query = obj.getJSONObject("Query_Request");
    	JSONObject para = query.getJSONObject("Parameter");
    	
    	switch(query.getString("Name")) {
    	
    	case "GetDataCountByTime":
    		result = getDataCountByTime(para);
    		Ack = "0";
    		Message = "success";
    		break;
    		
    	case "GetDataCountBySensor":
    		result = getDataCountBySensor(para);
    		Ack = "0";
    		Message = "success";
    		break;
    		
    	default:
    		System.out.println("No this query type");
    		Ack = "1";
    		Message = "No this query type";
    	}
    	
    //	int len = result.length();
    //	result = result + ", \"Ack\" : \"" + Ack + "\","
    //			+ "\"Message\" : \"" + Message + "\"},";
    //	String sum = jsonData + ", " + result + " }";
    	return result;
    }
    public String getDataCountByTime(JSONObject para) throws IOException
    {
    	String returnValue = "";
    	String start = para.getString("Start");
    	String end = para.getString("End")+1;
    	
    	Table t = this.connection.getTable(table);
    	Get get = new Get(Bytes.toBytes("row"));
    	Result rowResult = t.get(get);
    	
    	Map buffer = new HashMap();
    	
    	Scan scan = new Scan(Bytes.toBytes(start), Bytes.toBytes(end));
    	try (ResultScanner scanner = t.getScanner(scan)) {
    		for(Result result = scanner.next(); (result != null); result = scanner.next()) {
    			for(Cell cell : result.rawCells()) {
    				String rowkey = new String(cloneRow(cell));
    				if(!(buffer.containsKey(rowkey))){
    					buffer.put(rowkey, 1);
    				}
    			}
    		}
    	} // try
    	
    	int len = buffer.size();
    	String result = "{\"Result\" : \"" + len + "\"}";
    
    	return result;
    }
    
    public String getDataCountBySensor(JSONObject para) throws IOException
    {
    	String returnValue = "";
    	String start = para.getString("Start");
    	String end = para.getString("End");
    	String sensor = para.getString("Sensor_Name");
    	System.out.println("End :" + end +" Sensor : "+ sensor);
    	
    	
    	Table t = this.connection.getTable(table);
    	Get get = new Get(Bytes.toBytes("row"));
    	Result rowResult = t.get(get);
    	
    	Map buffer = new HashMap();
    	
    	Filter filter1 = new SingleColumnValueFilter(Bytes.toBytes("FIELD"), Bytes.toBytes(sensor), CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(start));
    	Filter filter2 = new SingleColumnValueFilter(Bytes.toBytes("FIELD"), Bytes.toBytes(sensor), CompareOp.LESS_OR_EQUAL, Bytes.toBytes(end));
    	FilterList filterlist = new FilterList();
    	filterlist.addFilter(filter1);
    	filterlist.addFilter(filter2);
    	
    	Scan scan = new Scan();

    	scan.setFilter(filterlist);
    	
    	try (ResultScanner scanner = t.getScanner(scan)) {
    		for(Result result = scanner.next(); (result != null); result = scanner.next()) {
    			for(Cell cell : result.rawCells()) {
    				String rowkey = new String(cloneRow(cell));
    				String value = new String(cloneValue(cell));
	    				
					if(!(buffer.containsKey(rowkey))){
						buffer.put(rowkey, 1);
						System.out.println(rowkey +"  "+ value);
						
					}

    			}
    		}
    	} // try
    	
    	int len = buffer.size();
    	String result = "{\"Result\" : \"" + len + "\"}";
    
    	return result;
    }
    
    public String getDataByTime(JSONObject para) throws IOException
    {
    	String returnValue = "";
    	String start = para.getString("Start");
    	String end = para.getString("End")+1;
    	
    	Table t = this.connection.getTable(table);
    	Get get = new Get(Bytes.toBytes("row"));
    	Result rowResult = t.get(get);
    	
    	Map sensorInfoMap = new HashMap();
    	Map sensorNumMap = new HashMap();
    	Map dataList = new HashMap();
    //	System.out.println("-------Start Scan------");
    	Scan scan = new Scan(Bytes.toBytes(start), Bytes.toBytes(end));
    	try (ResultScanner scanner = t.getScanner(scan)) {
    		for(Result result = scanner.next(); (result != null); result = scanner.next()) {
    			for(Cell cell : result.rawCells()) {
    				String family = new String(cloneFamily(cell));
    				String qualifier = new String(cloneQualifier(cell));
    				String value = new String(cloneValue(cell));
    				String rowkey = new String(cloneRow(cell));
    			//	System.out.println("-------Loop------");
    		//		System.out.println("family: "+family+" qualifier: "+qualifier+" value: "+value+" rowkey: "+rowkey);
    				
    				switch (family){
    				
	    				case "TYPE":
	    					//System.out.println("-------TYPE------");
	    					if(!(sensorInfoMap.containsKey(qualifier))){
		    					Map info = new HashMap();
		    					info.put("Type", value);
		    					info.put("FieldNumber", qualifier);
		    					sensorInfoMap.put(qualifier, info);
		    				}
	    					break;
	    				case "FIELD_NUM":
	    				//	System.out.println("-------FIELD_NUM------");
	    					if(!(sensorNumMap.containsKey(qualifier))){
	    						sensorNumMap.put(qualifier, value);
	    					}
	    					break;
	    				case "FIELD":
	    				//	System.out.println("-------FIELD------");
	    					if(!(dataList.containsKey(rowkey))){
		    					Map data = new HashMap();
		    					data.put(qualifier, value);
		    					dataList.put(rowkey, data);
	    					}else {
	    						Map data = (Map) dataList.get(rowkey);
	    						data.put(qualifier, value);
	    						dataList.put(rowkey, data);
	    					}
	    					break;
	    				default:
    				}
    				
    			}
    		}
    	} // try
    /*	System.out.println("-------Senso Num------");
    	System.out.println(sensorNumMap.toString());
    	System.out.println("-------Sensor Info------");
    	System.out.println(sensorInfoMap.toString());
    	System.out.println("-------datalist Info------");
    	System.out.println(dataList.toString()); */
    	
    	for(Object key : sensorInfoMap.keySet()){
    		
    		Map info = (Map) sensorInfoMap.get(key);
    		info.put("FieldNumber", sensorNumMap.get(key));
    		
    	}
    	
    	for(Object key : dataList.keySet()){
    		
    		Map sub_data = (Map) dataList.get(key);
    	//	System.out.println("Key: "+key);
    		Map replace_data = new HashMap();
        	for(Object key2 : sub_data.keySet()){
        	//	System.out.println(key2);
        		Map info = (Map) sensorInfoMap.get(key2);
        		replace_data.put(sensorNumMap.get(key2), sub_data.get(key2));
        	//	System.out.println(replace_data.toString());
        	}
        	dataList.put(key, replace_data);
    	}
    	
    	int i = 1;
    	Map finalDataList = new HashMap();
    	for(Object key : dataList.keySet()) {
    		
    		Map data = new HashMap();
    		data.put("Timestamp", key);
    		data.put("SenserValue", dataList.get(key));
    		finalDataList.put(i++, data);
    		
    	}
    	
    	Map all = new HashMap();
    	all.put("Result", sensorInfoMap);
    	all.put("Data_List", finalDataList);
    //	System.out.println("-------END------");
    //	System.out.println(all.toString());
    	JSONObject obj = new JSONObject(all);
    	returnValue = obj.toString();
    //	System.out.println(returnValue);
    	
    	return returnValue;
    }
    
    
    public void create( String tableName ) throws IOException 
    {
		HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
		HColumnDescriptor columnDesc1 = new HColumnDescriptor(Bytes.toBytes("FIELD"));
		HColumnDescriptor columnDesc2 = new HColumnDescriptor(Bytes.toBytes("TYPE"));
		HColumnDescriptor columnDesc3 = new HColumnDescriptor(Bytes.toBytes("FIELD_NUM"));

		tableDesc.addFamily(columnDesc1);
		tableDesc.addFamily(columnDesc2);
		tableDesc.addFamily(columnDesc3);
		HbaseOp.admin.createTable(tableDesc);;

    }
    
    public void close() throws IOException {
    	
	    HbaseOp.admin.close();
	    HbaseOp.connection.close();
 
    }
    
    public Map getNumNameMap4Json(JSONObject jsonObject){
     
        Iterator  keyIter = jsonObject.keys();
        String key;
        Object value;
        Map valueMap = new HashMap();

        while( keyIter.hasNext())
        {
            key = (String)keyIter.next();
            value = jsonObject.getJSONObject(key).get("FiledNumber");
            valueMap.put(value, key);
        }
        
        return valueMap;
    }
    
    public Map getNameTypeMap4Json(JSONObject jsonObject){
        
        Iterator  keyIter = jsonObject.keys();
        String key;
        Object value;
        Map valueMap = new HashMap();

        while( keyIter.hasNext())
        {
            key = (String)keyIter.next();
            value = jsonObject.getJSONObject(key).get("Type");
            valueMap.put(key, value);
        }
        
        return valueMap;
    }
    
    public List<JSONObject> getJSONList4Json(JSONObject data) {
    	
    	List<JSONObject> list = new ArrayList();

	    Iterator<?> keys = data.keys();
	    while( keys.hasNext() ) {
	    	String key = (String) keys.next();
	    	if ( data.get(key) instanceof JSONObject ) {
	    		list.add(data.getJSONObject(key));
	    	}
	    }
	    return list;
    }
    
    public Map getKeyValueMap4Json(JSONObject jsonObject) {
    	
        Iterator  keyIter = jsonObject.keys();
        String key;
        Object value;
        Map valueMap = new HashMap();

        while( keyIter.hasNext() )
        {
            key = (String)keyIter.next();
            value = jsonObject.get(key);
            valueMap.put(key, value);
        }
        
        return valueMap;
    }
    
    public void testWhile(List<JSONObject> data_list) {
    	
	    Iterator<JSONObject> i_it = data_list.iterator();
	    int i= 0 ;
	    int count = 0;
	    while(i_it.hasNext()) {
	    	
	    	JSONObject jobj = i_it.next();
	    	JSONObject sensor_value = jobj.getJSONObject("Sensor_Value");
	    	
	    	Map value_list = getKeyValueMap4Json(sensor_value);
	    //	JSONObject obj = (JSONObject) i_it.next();
	    	
	    	System.out.println(value_list.toString());
	    	for (Object key : value_list.keySet()){
			 //   String col = (String) sensorNumNameMap.get(key);
			    String value = (String) value_list.get(key);
			    String time = (String) jobj.getString("Timestamp");
			    String num = (String) key;
			 //   String type = (String) sensorNameTypeMap.get(col);
			    
			    System.out.println( "value: "+ value + "time :"+ time+ "num: "+num);
	    	} 
	    }
	    i++;
	    
    }
}