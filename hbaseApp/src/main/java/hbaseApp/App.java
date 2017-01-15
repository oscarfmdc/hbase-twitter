package hbaseApp;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class App 
{

	private String tableName = "twitterStats";

	/**
	 * 
	 * @param args mode zkHost startTS endTS N Languages dataFolder outputFolder
	 */
	public static void main(String[] args)
	{

		if (!(args.length > 0)){
			System.out.println("Incorrect arguments:");
			System.out.println("Arguments: mode zkHost startTS endTS N Languages dataFolder outputFolder");
			System.exit(1);
		}
		String mode = args[0];
		App hbaseApp = new App();
		switch (mode) {
		case "1": 	

			break;
		case "2":

			break;
		case "3": 	
			hbaseApp.topNFrequency(args);
			break;
		case "4": 	
			hbaseApp.createTable(args);            
			break;     	
		}
	}

	/**
	 * Finds Top-N most used words and the frequency of each word regardless of the 
	 * language in a time interval defined with the provided start and end timestamp
	 * Start and end timestamp are in milliseconds
	 * @param args mode	ZKHOST:ZKPORT startTS endTS N outputFolder
	 */
	private void topNFrequency(String[] args) 
	{
		// Checks arguments
		if (args.length != 6) {
			System.out.println("Incorrect arguments");
			System.out.println("Usage with mode 3: mode	ZKHOST:ZKPORT startTS endTS N outputFolder");
			System.exit(1);
		}

		String startTS = args[2];
		String endTS = args[3];

		byte[] initialKey = new byte[44];
		System.arraycopy(Bytes.toBytes(startTS), 0, initialKey, 0, startTS.length());
		byte[] finalKey = new byte[44];
		System.arraycopy(Bytes.toBytes(endTS), 0, finalKey, 0, endTS.length());

		Scan scan = new Scan(initialKey, finalKey);

		Configuration conf = HBaseConfiguration.create();
		byte[] table = Bytes.toBytes(tableName);
		try {
			HConnection conn = HConnectionManager.createConnection(conf);
			HTable hTable = new HTable(TableName.valueOf(table), conn);

			ResultScanner rs = hTable.getScanner(scan);
			Result res = rs.next();
			while (res != null && !res.isEmpty()){
				byte[] bTopic = res.getValue(Bytes.toBytes("en"), Bytes.toBytes("TOPIC"));
				byte[] bCount = res.getValue(Bytes.toBytes("en"), Bytes.toBytes("COUNT"));
				String topic = Bytes.toString(bTopic);
				String count = Bytes.toString(bCount);				
				System.out.println("Read " + topic + " " + count);
				res = rs.next();
			}

			hTable.close();

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	/**
	 * creates the table twitterStats in HBase and loads the data files from dataFolder
	 * @param args mode	ZKHOST:ZKPORT dataFolder
	 */
	private void createTable(String[] args)
	{
		// Checks arguments
		if (args.length != 3) {
			System.out.println("Incorrect arguments");
			System.out.println("Usage with mode 4: mode	ZKHOST:ZKPORT dataFolder");
			System.exit(1);
		}

		// Gets languages from files in dataFolder
		File dataDir = new File(args[2]);
		File[] files = dataDir.listFiles();
		ArrayList<String> languages = new ArrayList<String>();
		for (int i = 0; i < files.length; i++) {
			File currentFile = files[i];
			if (currentFile.isFile() && currentFile.getName().endsWith(".out")) {
				String language = currentFile.getName();
				languages.add(language.split(".out")[0]);
			}
		}

		// Creates a table in HBase
		Configuration conf = HBaseConfiguration.create();
		HBaseAdmin admin;
		byte[] table = Bytes.toBytes(tableName);

		try {
			admin = new HBaseAdmin(conf);
			if (!admin.tableExists(tableName)) {
				System.out.println("Creating table " + tableName);
				HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(table));
				for (int i = 0; i < languages.size(); i++) {
					System.out.println("Creating family " + languages.get(i));
					byte[] columnD = Bytes.toBytes(languages.get(i));
					HColumnDescriptor family = new HColumnDescriptor(columnD);
					tableDesc.addFamily(family);
				}
				admin.createTable(tableDesc);
				System.out.println("Table " + tableName + " created");
			} else {
				System.out.println("Table " + tableName + " already exists in HBase");
			}

			// Opens table
			HConnection conn = HConnectionManager.createConnection(conf);
			HTable hTable = new HTable(TableName.valueOf(table), conn);

			// Loads files to the HBase table
			for (int i = 0; i < files.length; i++) {
				File currentFile = files[i];
				if (currentFile.isFile() && currentFile.getName().endsWith(".out")) {
					BufferedReader br = new BufferedReader(new FileReader(currentFile));
					String line;
					while ((line = br.readLine()) != null) {
						// Parse each line
						// timestamp_ms,lang,topHT1,freqHT1,topHT2,freqHT2,topHT3,freqHT3
						String[] columns = line.split(",");			
						String timestamp = columns[0];
						String lang = columns[1];
						insert(timestamp, lang, columns[2], columns[3], hTable, "1");
						insert(timestamp, lang, columns[4], columns[5], hTable, "2");
						insert(timestamp, lang, columns[6], columns[7], hTable, "3");
					}
					br.close();
				} 
			}

			//			admin.disableTable(tableName);
			//			admin.deleteTable(tableName);

		} catch (MasterNotRunningException e1) {
			e1.printStackTrace();
		} catch (ZooKeeperConnectionException e1) {
			e1.printStackTrace();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}

	private void insert(String timestamp, String lang, String topic, String count, HTable hTable, String position) throws IOException
	{
		if (topic.equals("null") || count.equals("null")) {
			return;
		}

		byte[] key = new byte[44];
		System.arraycopy(Bytes.toBytes(timestamp), 0, key, 0, timestamp.length());
		System.arraycopy(Bytes.toBytes(lang), 0, key, 20, lang.length());
		System.arraycopy(Bytes.toBytes(position), 0, key, 20, position.length());

		Get get = new Get(key);
		Result res = hTable.get(get);

		if (res != null) {
			Put put = new Put(key);
			put.add(Bytes.toBytes(lang), Bytes.toBytes("TOPIC"), Bytes.toBytes(topic));
			put.add(Bytes.toBytes(lang), Bytes.toBytes("COUNT"), Bytes.toBytes(count));
			hTable.put(put);

			System.out.println("Inserted " + topic + " " + count);

		}
	}
}
