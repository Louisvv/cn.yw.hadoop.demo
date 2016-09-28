package com.yanwei.hbase.demo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;


/*
 * 2016.9.26
 * yanwei
 * hbase api develop
 * 
 */
public class HbaseDemo1 {

	public static void main(String[] args) throws IOException {
		Configuration conf=HBaseConfiguration.create();//创建所需配置
		conf.set("hbase.zookeeper.quorum", "192.168.47.204");
		conf.set("hbase.zookeeper.property.clientport", "2181");
		
		HTable table=new HTable(conf, "texttable");//实例化一个新的客户端
		
		Put put=new Put(Bytes.toBytes("row-3"));//指定一行来创建一个put
		
		
		put.add(Bytes.toBytes("info"), Bytes.toBytes("name"), 
				Bytes.toBytes("yw"));//向put中添加一个名为:conlum1:qual1的列
		put.add(Bytes.toBytes("info"), Bytes.toBytes("age"),
				Bytes.toBytes("23"));//向put中添加一个名为:conlum1:qual2的列
		put.add(Bytes.toBytes("info"), Bytes.toBytes("high"),
				Bytes.toBytes("180"));//向put中添加一个名为:conlum1:qual2的列
		
		table.put(put);//将这一行存储到hbase表中

	}

}
