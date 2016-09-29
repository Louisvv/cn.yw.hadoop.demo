package com.yanwei.hbase.demo;


import java.io.IOException;       
import java.util.ArrayList;       
import java.util.List;       
        
import org.apache.hadoop.conf.Configuration;       
import org.apache.hadoop.hbase.HBaseConfiguration;       
import org.apache.hadoop.hbase.HColumnDescriptor;       
import org.apache.hadoop.hbase.HTableDescriptor;       
import org.apache.hadoop.hbase.KeyValue;       
import org.apache.hadoop.hbase.MasterNotRunningException;       
import org.apache.hadoop.hbase.ZooKeeperConnectionException;       
import org.apache.hadoop.hbase.client.Delete;       
import org.apache.hadoop.hbase.client.Get;       
import org.apache.hadoop.hbase.client.HBaseAdmin;       
import org.apache.hadoop.hbase.client.HTable;       
import org.apache.hadoop.hbase.client.Result;       
import org.apache.hadoop.hbase.client.ResultScanner;       
import org.apache.hadoop.hbase.client.Scan;       
import org.apache.hadoop.hbase.client.Put;       
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.thrift.generated.Hbase.Processor.scannerClose;
import org.apache.hadoop.hbase.util.Bytes;       
import org.apache.hadoop.hive.ql.parse.HiveParser.tableName_return;
        
public class HbaseDemo2 {         
           
    private static Configuration conf =null;    
     /**  
      * ��ʼ������  
     */    
     static {    
         conf = HBaseConfiguration.create(); 
         conf.set("hbase.zookeeper.quorum", "192.168.47.204");
		 conf.set("hbase.zookeeper.property.clientport", "2181");
     }    
         
    /**    
     * ����һ�ű�    
     */      
    public static void creatTable(String tableName, String[] familys) throws Exception {       
        HBaseAdmin admin = new HBaseAdmin(conf);       
        if (admin.tableExists(tableName)) {       
            System.out.println("table already exists!");       
        } else {       
            HTableDescriptor tableDesc = new HTableDescriptor(tableName);       
            for(int i=0; i<familys.length; i++){       
                tableDesc.addFamily(new HColumnDescriptor(familys[i]));       
            }       
            admin.createTable(tableDesc);       
            System.out.println("create table " + tableName + " ok.");       
        }        
    }       
           
    /**    
     * ɾ����    
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
            
    /**    
     * ����һ�м�¼    
     */      
    public static void addRecord (String tableName, String rowKey, String family, String qualifier, String value)       
            throws Exception{       
        try {       
            HTable table = new HTable(conf, tableName);       
            Put put = new Put(Bytes.toBytes(rowKey));       
            put.add(Bytes.toBytes(family),Bytes.toBytes(qualifier),Bytes.toBytes(value));       
            table.put(put);       
            System.out.println("insert recored " + rowKey + " to table " + tableName +" ok.");       
        } catch (IOException e) {       
            e.printStackTrace();       
        }       
    }       
        
    /**    
     * ɾ��һ�м�¼    
     */      
    public static void delRecord (String tableName, String rowKey) throws IOException{       
        HTable table = new HTable(conf, tableName);       
        List list = new ArrayList();       
        Delete del = new Delete(rowKey.getBytes());       
        list.add(del);       
        table.delete(list);       
        System.out.println("del recored " + rowKey + " ok.");       
    }       
            
    /**    
     * ����һ�м�¼    
     */      
    public static void getOneRecord (String tableName, String rowKey) throws IOException{       
        HTable table = new HTable(conf, tableName);       
        Get get = new Get(rowKey.getBytes());       
        Result rs = table.get(get);       
        for(KeyValue kv : rs.raw()){       
            System.out.print(new String(kv.getRow()) + " " );       
            System.out.print(new String(kv.getFamily()) + ":" );       
            System.out.print(new String(kv.getQualifier()) + " " );       
            System.out.print(kv.getTimestamp() + " " );       
            System.out.println(new String(kv.getValue()));       
        }       
    }       
            
    /**    
     * ��ʾ��������    
     */      
    public static void getAllRecord (String tableName) {       
        try{       
             HTable table = new HTable(conf, tableName);       
             Scan s = new Scan();       
             ResultScanner ss = table.getScanner(s);       
             for(Result r:ss){       
                 for(KeyValue kv : r.raw()){       
                    System.out.print(new String(kv.getRow()) + " ");       
                    System.out.print(new String(kv.getFamily()) + ":");       
                    System.out.print(new String(kv.getQualifier()) + " ");       
                    System.out.print(kv.getTimestamp() + " ");       
                    System.out.println(new String(kv.getValue()));       
                 }       
             }       
        } catch (IOException e){       
            e.printStackTrace();       
        }       
    }       
    /**
     * ��ȡĳ�����ݵĶ���汾
     */
    public static void getResultByVersion(String tableName, String rowKey,  
            String familyName, String columnName) throws IOException {  
        HTable table = new HTable(conf, Bytes.toBytes(tableName));  
        Get get = new Get(Bytes.toBytes(rowKey));  
        get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName));  
        get.setMaxVersions(5);  
        Result result = table.get(get);  
        for (KeyValue kv : result.list()) {  
            System.out.println("family:" + Bytes.toString(kv.getFamily()));  
            System.out  
                    .println("qualifier:" + Bytes.toString(kv.getQualifier()));  
            System.out.println("value:" + Bytes.toString(kv.getValue()));  
            System.out.println("Timestamp:" + kv.getTimestamp());  
            System.out.println("-------------------------------------------");  
        }  
           
    public static void  main (String [] agrs) {       
        try {       
            String tableName = "texttable";       
            String[] familys = {"info", "data"};       
            //HbaseDemo2.creatTable(tablename, familys);       
                    
            //add record zkb       
//            HbaseDemo2.addRecord(tablename,"row-3","info","name","yw");       
//            HbaseDemo2.addRecord(tablename,"row-3","info","age","23");       
//            HbaseDemo2.addRecord(tablename,"row-3","info","higt","180");       
//            HbaseDemo2.addRecord(tablename,"row-3","data","sex","nan");       
            //add record  baoniu       
//            HbaseDemo2.addRecord(tablename,"baoniu","grade","","4");       
//            HbaseDemo2.addRecord(tablename,"baoniu","course","math","89");       
                   
//            
//            System.out.println("===========get one record========");       
//            HbaseDemo2.getOneRecord(tableName, "row-1");       
           
            System.currentTimeMillis();
            
            /* ������
            HTable table=new HTable(conf, tableName);
            Scan scan =new Scan();
            Filter filter1=new  RowFilter(CompareOp.LESS_OR_EQUAL,
            		new BinaryComparator(Bytes.toBytes("row-2")));
            scan.setFilter(filter1);
            ResultScanner rs=table.getScanner(scan);
            for (Result result : rs) {
				System.out.println(result);
			}
            rs.close();
            */
            
            
//            System.out.println("===========show all record========");       
//            HbaseDemo2.getAllRecord(tablename);       
                    
         
                    
            
        } catch (Exception e) {       
            e.printStackTrace();       
        }       
    }       
}   