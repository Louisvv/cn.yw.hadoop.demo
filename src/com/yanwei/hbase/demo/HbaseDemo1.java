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
		
		HTable table=new HTable(conf, "texttable");//ʵ����һ���µĿͻ���
		
		Put put=new Put(Bytes.toBytes("row1"));//ָ��һ��������һ��put
		
		
		put.add(Bytes.toBytes("conlum1"), Bytes.toBytes("qual1"), 
				Bytes.toBytes("val1"));//��put�����һ����Ϊ:conlum1:qual1����
		put.add(Bytes.toBytes("conlum1"), Bytes.toBytes("qual2"),
				Bytes.toBytes("val2"));//��put�����һ����Ϊ:conlum1:qual2����
		
		table.put(put);//����һ�д洢��hbase����

	}

}
