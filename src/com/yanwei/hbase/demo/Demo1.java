package com.yanwei.hbase.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

/** 
 * @author ãÆÍþ
 * hbase:¼òµ¥ÅäÖÃÊµÀý
 * @version 2016-10-17 ÏÂÎç04:45:21
 */
public class Demo1 	extends Thread{
	public Configuration conf;
	public HTable table;
	public HBaseAdmin admin;
	
	public Demo1(){
		conf=HBaseConfiguration.create();
		conf.set("hbase.master", "hadoop1:60010");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("hbase.zookeeper.quorum", "hadoop1");
		try {
			table = new HTable(conf, Bytes.toBytes("text"));
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
		
}
	
