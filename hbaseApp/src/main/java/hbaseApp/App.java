package hbaseApp;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

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
		int numResults = Integer.parseInt(args[4]);
		String outputFolder = args[5];

		byte[] initialKey = new byte[44];
		System.arraycopy(Bytes.toBytes(startTS), 0, initialKey, 0, startTS.length());
		byte[] finalKey = new byte[44];
		System.arraycopy(Bytes.toBytes(endTS), 0, finalKey, 0, endTS.length());

		HashMap<String, Long> results = new HashMap<String,Long>();

		Configuration conf = HBaseConfiguration.create();
		byte[] table = Bytes.toBytes(tableName);
		try {
			HConnection conn = HConnectionManager.createConnection(conf);
			HTable hTable = new HTable(TableName.valueOf(table), conn);

			int numFamilies = hTable.getTableDescriptor().getColumnFamilies().length;
			for (int i = 0; i < numFamilies; i++) { // iterates column families
				Scan scan = new Scan(initialKey, finalKey);
				byte[] familyName = hTable.getTableDescriptor().getColumnFamilies()[i].getName();
				scan.addFamily(familyName);

				ResultScanner rs = hTable.getScanner(scan);
				Result res = rs.next();
				while (res != null && !res.isEmpty()){
					byte[] bTopic = res.getValue(familyName, Bytes.toBytes("TOPIC"));
					byte[] bCount = res.getValue(familyName, Bytes.toBytes("COUNT"));
					String topic = Bytes.toString(bTopic);
					String count = Bytes.toString(bCount);
					if (results.get(topic) == null) {
						results.put(topic, Long.parseLong(count));
					}
					else {
						long oldValue = results.get(topic);
						results.put(topic, oldValue + Long.parseLong(count));
					}

					res = rs.next();
				}
			}

			hTable.close();

		} catch (IOException e) {
			e.printStackTrace();
		}

		printTopN(startTS, endTS, numResults, outputFolder, results);

	}

	/**
	 * Prints topN results in output file
	 * @param startTS
	 * @param endTS
	 * @param numResults
	 * @param outputFolder
	 * @param results
	 */
	private void printTopN(String startTS, String endTS, int numResults, String outputFolder, HashMap<String, Long> results){
		Set<Entry<String, Long>> resultsSet = results.entrySet();
		List<Entry<String, Long>> resultsList = new ArrayList<Entry<String, Long>>(resultsSet);
		Collections.sort(resultsList, new Comparator<Map.Entry<String, Long>>() {
			public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {
				if(o2.getValue() > o1.getValue()) {
					return 1;
				} else if (o2.getValue() < o1.getValue()) {
					return -1;
				} else if (o2.getValue() == o1.getValue()) {
					return o1.getKey().compareTo(o2.getKey());
				} else {
					return 0;
				}
			}
		});

		File outputFile = new File(outputFolder + "/13_query3.out");
		BufferedWriter writer;
		try {
			writer = new BufferedWriter(new FileWriter(outputFile, true));
			for (int i = 0; i < numResults && i < resultsList.size(); i++) {
				writer.append(i + "," + resultsList.get(i).getKey() + "," + startTS + "," + endTS);
				writer.newLine();

				//System.out.println(i + "," + resultsList.get(i).getKey() + "," + startTS + "," + endTS);
			}
			writer.close();
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
				//System.out.println("Creating table " + tableName);
				HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(table));
				for (int i = 0; i < languages.size(); i++) {
					//System.out.println("Creating family " + languages.get(i));
					byte[] columnD = Bytes.toBytes(languages.get(i));
					HColumnDescriptor family = new HColumnDescriptor(columnD);
					tableDesc.addFamily(family);
				}
				admin.createTable(tableDesc);
				//System.out.println("Table " + tableName + " created");
			} else {
				//System.out.println("Table " + tableName + " already exists in HBase");
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

			//	admin.disableTable(tableName);
			//	admin.deleteTable(tableName);

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

			//System.out.println("Inserted " + topic + " " + count);
		}
	}
}
