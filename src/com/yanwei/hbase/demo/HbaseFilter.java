package com.yanwei.hbase.demo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseFilter {

	private static Configuration conf = null;
	/**
	 * ≥ı ºªØ≈‰÷√
	 */
	static {
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "192.168.47.204");
		conf.set("hbase.zookeeper.property.clientport", "2181");
	}

	public static void addRecord(String tableName, String rowKey,
			String family, String qualifier, String value) throws Exception {
		try {
			HTable table = new HTable(conf, tableName);
			Put put = new Put(Bytes.toBytes(rowKey));
			put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier),
					Bytes.toBytes(value));
			table.put(put);
			System.out.println("insert recored " + rowKey + " to table "
					+ tableName + " ok.");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
			String tableName="text1";
			
	}

}
