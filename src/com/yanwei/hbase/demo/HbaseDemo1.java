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
		Configuration conf=HBaseConfiguration.create();//������������
		conf.set("hbase.zookeeper.quorum", "192.168.47.204");
		conf.set("hbase.zookeeper.property.clientport", "2181");
		
		HTable table=new HTable(conf, "texttable");//ʵ����һ���µĿͻ���
		
		Put put=new Put(Bytes.toBytes("row-3"));//ָ��һ��������һ��put
		
		
		put.add(Bytes.toBytes("info"), Bytes.toBytes("name"), 
				Bytes.toBytes("yw"));//��put�����һ����Ϊ:conlum1:qual1����
		put.add(Bytes.toBytes("info"), Bytes.toBytes("age"),
				Bytes.toBytes("23"));//��put�����һ����Ϊ:conlum1:qual2����
		put.add(Bytes.toBytes("info"), Bytes.toBytes("high"),
				Bytes.toBytes("180"));//��put�����һ����Ϊ:conlum1:qual2����
		
		table.put(put);//����һ�д洢��hbase����

	}

}
