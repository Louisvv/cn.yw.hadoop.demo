package com.yanwei.hdfs.demo;

import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class GetFile {

	/**
	 * 闫威
	 * 16.10.12
	 * HDFS获取文件
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
	
		Configuration conf=new Configuration();
	
		FileSystem hdfs = FileSystem.get(conf);	
		
		org.apache.hadoop.fs.Path src =new org.apache.hadoop.fs.Path("/yw");
		
		Path dst=new Path("C:\\Users\\yanwei\\Desktop");
		
		hdfs.copyToLocalFile(src, dst);
	
		
		
	}

}
