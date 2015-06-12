package hbase_operator.hbase_operator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Hello world!
 *
 */
public class hbase_op 
{
    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
    }
    
    public static void put( String[] args ) throws IOException
    {
    		
	    Configuration conf = HBaseConfiguration.create();
	    Connection conn = ConnectionFactory.createConnection(conf);
	    Admin admin = conn.getAdmin();
	    
	    if(args[0] == null) {
	      System.out.println("table name not found...");
	    }
	    
	    TableName table = TableName.valueOf("metrology");
	    
	    Table t = conn.getTable(table);
	    
	    List<Put> lp = new ArrayList<Put>();
	    for(int i = 0; i < 2; i++) {
	      Put p = new Put(Bytes.toBytes(i + ""));
	      String d = "2014-01-09 12:09:31.000";
	      p.addColumn(Bytes.toBytes("CF"), Bytes.toBytes("DD"), Bytes.toBytes(d));
	      lp.add(p);
	    }	
	    
	    t.put(lp);
	    t.close();
	    admin.close();
	    conn.close();
	    
	  }
	  


    	
    	
}