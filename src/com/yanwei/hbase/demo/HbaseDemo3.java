package com.yanwei.hbase.demo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class HbaseDemo3 {
		private static Configuration conf = null;
		static {
			conf= HBaseConfiguration.create();
			conf.set("hbase.zookeeper.quorum", "192.168.47.204");
			conf.set("hbase.zookeeper.property.clientport", "2181");
		}
		private static boolean isExist(String tablename) throws IOException{
			HBaseAdmin hbadmin=new HBaseAdmin(conf);
			return hbadmin.tableExists(tablename);
		}
		
}






