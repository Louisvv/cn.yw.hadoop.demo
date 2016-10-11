package com.yanwei.hdfs.demo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ThreadCopyFile implements Runnable{
	
	public Path src;
	public Path dst;
	public String name;
	public ThreadCopyFile(Path path, String string) {
		// TODO Auto-generated constructor stub
	}

	public ThreadCopyFile(Path src,Path dst,String name) {
		this.src=src;
		this.dst=dst;
		this.name=name;
	}
	
	@Override
	public void run() {
		try {
			
			copy(src, dst);
			show();
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

	public void copy(Path src,Path dst) throws IOException {  
		Configuration conf=new Configuration();
        FileSystem hdfs=FileSystem.get(conf);  
		hdfs.copyFromLocalFile(src, dst);
        System.out.println("Upload to"+conf.get("fs.default.name"));  
      
        FileStatus files[]=hdfs.listStatus(dst);
        for(FileStatus file:files){
            System.out.println(file.getPath());
        }
	}
	public void show() {
		 for (int i = 0; i < 3; i++) {
				System.out.println(name+"。。。"+i);
				System.out.println("现在"+name);
			}
	}
	
	public static void main(String[] args) throws IOException {
            
		ThreadCopyFile tcf1=new ThreadCopyFile(new Path("E:\\大数据技术\\access_2013_05_31.log"),new Path("/pic1"), "c");
		ThreadCopyFile tcf2=new ThreadCopyFile(new Path("E:\\大数据技术\\access_2013_05_31.log"),new Path("/pic2"), "b");
		ThreadCopyFile tcf3=new ThreadCopyFile(new Path("E:\\大数据技术\\access_2013_05_31.log"),new Path("/pic3"), "a");
		
		Thread t1=new Thread(tcf1);
		Thread t2=new Thread(tcf2);
		Thread t3=new Thread(tcf3);
		
		
		t1.start();
		t2.start();
		t3.start();
		
	}
	
}
