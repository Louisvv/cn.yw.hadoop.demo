package com.yanwei.hbase.demo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseDemo3 {
		private static Configuration conf = null;
		static {
			conf= HBaseConfiguration.create();
			conf.set("hbase.zookeeper.quorum", "192.168.47.204");
			conf.set("hbase.zookeeper.property.clientport", "2181");
		}
		private static boolean isExist(String tableName) throws IOException{
			HBaseAdmin hbadmin=new HBaseAdmin(conf);
			return hbadmin.tableExists(tableName);
		}
		
		public static void createtable(String tableName) {
			System.out.println("开始创建表");
			try {
				HBaseAdmin ha=new HBaseAdmin(conf);
				if(ha.tableExists(tableName)){
				System.out.println("该表已存在");
				}
				
					/*ha.disableTable(tableName);
					ha.deleteTable(tableName);
					System.out.println(tableName+"一存在，将该表删除");
				}*/
				HTableDescriptor tableDescriptor= new HTableDescriptor(tableName);
				tableDescriptor.addFamily(new HColumnDescriptor("info"));
				tableDescriptor.addFamily(new HColumnDescriptor("data"));
				ha.createTable(tableDescriptor);
															
			} catch (MasterNotRunningException e) {
				e.printStackTrace();
			} catch (ZooKeeperConnectionException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		/**
		 * 添加数据
		 */
		public static void addData(String tableName,String row,String columnFamily,String column,String value) throws Exception {
			System.out.println("开始插入数据");
			HTable table=new HTable(conf, tableName);
			Put put= new Put(Bytes.toBytes(row));
			put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
			table.put(put);				
		}
		
		
		/**
		 * 获取一行数据
		 */
		public static void  getRow(String tableName,String row) throws Exception {
			HTable table=new HTable(conf, tableName);
			Get get=new Get(Bytes.toBytes(row));
			Result result=table.get(get);
			for(KeyValue rowKV:result.raw()){
				System.out.println("行名:"+new String(rowKV.getRow())+" ");
				System.out.println("时间戳:"+rowKV.getTimestamp()+" ");
				System.out.print("列族名:" + new String(rowKV.getFamily()) + " ");
		        System.out.print("列名:" + new String(rowKV.getQualifier()) + " ");
		        System.out.println("值:" + new String(rowKV.getValue()));
			}
		}
		
		/**
		 * 查看表中所有数据
		 */
		public static void getAll(String tableName) {
				try {
					HTable ha=new HTable(conf, tableName);
					Scan sc=new Scan();
					ResultScanner rs=ha.getScanner(sc);
					for (Result result : rs) {
						for (KeyValue kv : result.raw()) {
							//System.out.println(new String(kv.getValue()));
							System.out.print(new String(kv.getRow()) + " ");       
		                    System.out.print(new String(kv.getFamily()) + ":");       
		                    System.out.print(new String(kv.getQualifier()) + " ");       
		                    System.out.print(kv.getTimestamp() + " ");       
		                    System.out.println(new String(kv.getValue()));   
						}
					}						
				} catch (IOException e) {
					e.printStackTrace();
				}		
		}
		
		/**
		 * 删除表
		 */
		 public static void deleteTable(String tableName) throws Exception {       
		       try {       
		           HBaseAdmin admin = new HBaseAdmin(conf);       
		           admin.disableTable(tableName);       
		           admin.deleteTable(tableName);       
		           System.out.println("delete table " + tableName + " ok.");       
		       } catch (MasterNotRunningException e) {       
		           e.printStackTrace();       
		       } catch (ZooKeeperConnectionException e) {       
		           e.printStackTrace();       
		       }       
		    }       
		 
		 
		 
//		public void  filter(String tableName) {
//				Filter fl=new RowFilter(CompareOp.LESS_OR_EQUAL);
//		}
//		
		
		
		
		
		public static void main(String[] args) {
				String tableName="text";
				String[] columnFamilys={"info","data"};
			//	HbaseDemo3.createtable(tableName);
				try {
					if (isExist(tableName)) {
						
						//第一个人
//						HbaseDemo3.addData(tableName, "row-1", "info", "name", "yw");
//						HbaseDemo3.addData(tableName, "row-1", "info", "age", "22");
//						HbaseDemo3.addData(tableName, "row-1", "info", "sex", "nan");
//						HbaseDemo3.addData(tableName, "row-1", "data", "high", "181");
						//第二个人
						/*HbaseDemo3.addData(tableName, "row-2", "info", "name", "why");
						HbaseDemo3.addData(tableName, "row-2", "info", "age", "23");
						HbaseDemo3.addData(tableName, "row-2", "info", "sex", "nv");
						HbaseDemo3.addData(tableName, "row-2", "data", "high", "140");*/
						
					   System.out.println("**************获取一条数据*************");
		                HbaseDemo3.getRow(tableName, "row-1");
		                HbaseDemo3.getRow(tableName, "row-2");
						HbaseDemo3.getAll("text");
				
						
					}
				} catch (Exception e) { 
					e.printStackTrace();
				}
		}
}






