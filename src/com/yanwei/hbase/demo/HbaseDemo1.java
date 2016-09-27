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
		
		HTable table=new HTable(conf, "texttable");//实例化一个新的客户端
		
		Put put=new Put(Bytes.toBytes("row1"));//指定一行来创建一个put
		
		
		put.add(Bytes.toBytes("conlum1"), Bytes.toBytes("qual1"), 
				Bytes.toBytes("val1"));//向put中添加一个名为:conlum1:qual1的列
		put.add(Bytes.toBytes("conlum1"), Bytes.toBytes("qual2"),
				Bytes.toBytes("val2"));//向put中添加一个名为:conlum1:qual2的列
		
		table.put(put);//将这一行存储到hbase表中

	}

}
