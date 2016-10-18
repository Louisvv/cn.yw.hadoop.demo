package com.yanwei.hbase.demo;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.generated.master.table_jsp;
import org.apache.hadoop.hbase.util.Bytes;

/** 
 * @author ãÆÍþ
 * hbase:¼òµ¥ÅäÖÃÊµÀý
 * @version 2016-10-17 ÏÂÎç04:45:21
 */
public class Demo1 	extends Thread{
	
		
	public static void main(String[] args) {
		Configuration conf;
		HTable table;
		HBaseAdmin admin;
		
			conf=HBaseConfiguration.create();
			conf.set("hbase.zookeeper.quorum", "192.168.47.204");
			conf.set("hbase.zookeeper.property.clientport", "2181");
			
			try {
				table = new HTable(conf, Bytes.toBytes("text"));
				Scan scanner=new Scan();
				ResultScanner rs=table.getScanner(scanner);
				for (Result result : rs) {
					List<KeyValue> list=result.list();
					for (KeyValue kv : list) {
						//System.out.println(new String(kv.getValue()));
					/*	System.out.print(new String(kv.getRow()) + " ");       
	                    System.out.print(new String(kv.getFamily()) + ":");       
	                    System.out.print(new String(kv.getQualifier()) + " ");       
	                    System.out.print(kv.getTimestamp() + " ");       
	                    System.out.println(new String(kv.getValue()));   */
						System.out.println(kv);
						
					}
				}
				
			
			} catch (Exception e) {
				e.printStackTrace();
			}
			
		}
	}

	
