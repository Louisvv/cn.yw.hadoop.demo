package com.yanwei.hdfs.demo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ThreadCopyFile implements Runnable{
	
	public Path src;
	public Path dst;
	
	

	public ThreadCopyFile(Path src,Path dst) {
		this.src=src;
		this.dst=dst;
		
	}
	
	@Override
	public void run() {
		try {
			
			copy(src, dst);
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 闫威
	 * 16.10.11
	 * 多线程上传文件
	 * @throws IOException 
	 */

	public synchronized void  copy(Path src,Path dst) throws IOException {  
		Configuration conf=new Configuration();
        FileSystem hdfs=FileSystem.get(conf);  
		hdfs.copyFromLocalFile(src, dst);
        System.out.println("开始上传文件");
		System.out.println("Upload to"+conf.get("fs.default.name"));
        
		
       
        FileStatus files[]=hdfs.listStatus(dst);
        for(FileStatus file:files){
            System.out.println(file.getPath());
            System.out.println("你好");
        }
        
	}
	
	
	public static void main(String[] args) throws IOException {
            

		 Path src=new Path("C:\\Users\\yanwei\\Desktop\\1.mp4");	
		 
		 Path dst=new Path("/pic1/1.mp4"); 			
		 
		 ThreadCopyFile tcf1=new ThreadCopyFile(src,dst);	
		
		 Thread t1=new Thread(tcf1);	
		
		 t1.start();
	
		
	}
	
}
