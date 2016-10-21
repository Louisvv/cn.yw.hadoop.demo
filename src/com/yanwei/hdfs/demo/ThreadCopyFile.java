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
		 Path src2=new Path("C:\\Users\\yanwei\\Desktop\\2.mp4");
		 Path src3=new Path("C:\\Users\\yanwei\\Desktop\\3.mp4");
		 Path src4=new Path("C:\\Users\\yanwei\\Desktop\\4.mp4");
		 Path src5=new Path("C:\\Users\\yanwei\\Desktop\\5.mp4");
		 Path src6=new Path("C:\\Users\\yanwei\\Desktop\\6.mp4");
		 Path src7=new Path("C:\\Users\\yanwei\\Desktop\\7.mp4");
		 Path src8=new Path("C:\\Users\\yanwei\\Desktop\\8.mp4");
		 Path src9=new Path("C:\\Users\\yanwei\\Desktop\\9.mp4");
		 Path src10=new Path("C:\\Users\\yanwei\\Desktop\\10.mp4");
		 
		 Path dst=new Path("/pic1/1.mp4");
		 Path dst1=new Path("/pic1/2.mp4");
		 Path dst2=new Path("/pic1/3.mp4");
		 Path dst3=new Path("/pic1/4.mp4");
		 Path dst4=new Path("/pic1/5.mp4");
		 Path dst5=new Path("/pic1/6.mp4");
		 Path dst6=new Path("/pic1/7.mp4");
		 Path dst7=new Path("/pic1/8.mp4");
		 Path dst8=new Path("/pic1/9.mp4");
		 Path dst9=new Path("/pic1/10.mp4");
		 
		 
		
		
		ThreadCopyFile tcf1=new ThreadCopyFile(src,dst1);
		ThreadCopyFile tcf2=new ThreadCopyFile(src2,dst2);
		ThreadCopyFile tcf3=new ThreadCopyFile(src3,dst3);
		ThreadCopyFile tcf4=new ThreadCopyFile(src4,dst4);
		ThreadCopyFile tcf5=new ThreadCopyFile(src5,dst5);
		ThreadCopyFile tcf6=new ThreadCopyFile(src6,dst6);
		ThreadCopyFile tcf7=new ThreadCopyFile(src7,dst7);
		ThreadCopyFile tcf8=new ThreadCopyFile(src8,dst8);
		ThreadCopyFile tcf9=new ThreadCopyFile(src9,dst9);
		ThreadCopyFile tcf10=new ThreadCopyFile(src10,dst);
		
		
		Thread t1=new Thread(tcf1);
		Thread t2=new Thread(tcf2);
		Thread t3=new Thread(tcf3);
		Thread t4=new Thread(tcf4);
		Thread t5=new Thread(tcf5);
		Thread t6=new Thread(tcf6);
		Thread t7=new Thread(tcf7);
		Thread t8=new Thread(tcf8);
		Thread t9=new Thread(tcf9);
		Thread t10=new Thread(tcf10);
		
		t1.start();
		t2.start();
		t3.start();
		t4.start();
		t5.start();
		t6.start();
		t7.start();
		t8.start();
		t9.start();
		t10.start();
		
	}
	
}
