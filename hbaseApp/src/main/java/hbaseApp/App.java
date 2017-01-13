package hbaseApp;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class App 
{
	/**
	 * 
	 * @param args mode zkHost startTS endTS N Languages dataFolder outputFolder
	 */
	public static void main( String[] args )
	{
		// Creates a table in HBase
		Configuration conf = HBaseConfiguration.create();
		HBaseAdmin admin;

		try {
			admin = new HBaseAdmin(conf);
			byte[] table = Bytes.toBytes("Users");
			byte[] columnD = Bytes.toBytes("BasicData");
			if(!admin.tableExists("Users")){				
				HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(table));
				HColumnDescriptor family = new HColumnDescriptor(columnD);
				family.setMaxVersions(10); // Default is 3.
				tableDesc.addFamily(family);
				admin.createTable(tableDesc);
				System.out.println("Table Users created");
			} else {
				System.out.println("Table Users already exists in HBase");
			}

			// Opens a table
			HConnection conn = HConnectionManager.createConnection(conf);
			HTable hTable = new HTable(TableName.valueOf(table),conn);

			// Inserts in a table
			byte[] key = Bytes.toBytes("Oscar");
			byte[] value = Bytes.toBytes("Madrid");
			byte[] column = Bytes.toBytes("Province");
			Put put = new Put(key);
			put.add(columnD, column, value);
			hTable.put(put);
			//HTable.put(List<Put> puts); 

			// Get from table
			Get get = new Get(key);
			Result result = hTable.get(get);
			byte[] province = result.getValue(columnD, column);
			System.out.println(new String(province));

			// Delete
			Delete delete = new Delete(key);
			hTable.delete(delete);

			// Close table
			hTable.close();


		} catch (MasterNotRunningException e1) {
			e1.printStackTrace();
		} catch (ZooKeeperConnectionException e1) {
			e1.printStackTrace();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}
}
