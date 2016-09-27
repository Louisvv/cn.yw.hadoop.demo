package com.yanwei.MapReduceDemo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class FsDemo{ 	
	
	static String  text ="hello world goodbye name \n" + "hello hadoop goodbye hadoop";

	public static void main(String[] args) throws Exception {

		Configuration conf=new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path file = new Path("text1.txt");
		FSDataOutputStream fsout =fs.create(file);
		try {
			fsout.write(text.getBytes());
		} finally{
			IOUtils.closeStream(fsout);
		}
	}

}
