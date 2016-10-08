package com.yanwei.hbase.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseManager extends Thread{
	public Configuration conf;
	public HTable table;
	public HBaseAdmin admin;
	
	public HbaseManager() {
		conf=HBaseConfiguration.create();
		conf.set("hbase.master", "haodop1:60000");
		conf.set("hbase.zookeeper.property.clientport", "2181");
		conf.set("hbase.zookeeper.quorum", "haodop1");
		try {
		table=new HTable(conf, Bytes.toBytes("texttable"));
		admin=new HBaseAdmin(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	} 
}
