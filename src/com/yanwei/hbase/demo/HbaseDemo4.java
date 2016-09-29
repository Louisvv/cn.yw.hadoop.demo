package com.yanwei.hbase.demo;

import java.io.IOException;
import java.util.List;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseDemo4 {

	/**
	 * 2016.9.29
	 * 闫威
	 * hbase列多个版本查询操作
	 * 
	 */
	public static void main(String[] args) {
		Configuration conf= HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "192.168.47.204");
		conf.set("hbase.zookeeper.property.clientport", "2181");
		try {
			HTable table=new HTable(conf, "text");
			Put put=new Put(Bytes.toBytes("row-1"));
			put.add(Bytes.toBytes("info"),Bytes.toBytes ("age"), Bytes.toBytes("23"));
			Scan scan =new Scan();
			Get get=new Get(Bytes.toBytes("row-1"));
			get.setMaxVersions(2);
			Result result=table.get(get);
			byte[] b=result.getValue(Bytes.toBytes("info"), Bytes.toBytes("age"));
			List<KeyValue> kv=result.getColumn(Bytes.toBytes("info"), Bytes.toBytes("age"));
			
			//System.out.println(kv);
			for(KeyValue rowKV:result.list()){
				System.out.println("行名:"+new String(rowKV.getRow())+" ");
				System.out.println("时间戳:"+rowKV.getTimestamp()+" ");
				System.out.print("列族名:" + new String(rowKV.getFamily()) + " ");
		        System.out.print("列名:" + new String(rowKV.getQualifier()) + " ");
		        System.out.println("值:" + new String(rowKV.getValue()));
			}
			
			
			
			
			
		} catch (IOException e) {
			
			e.printStackTrace();
		}

	}

}
