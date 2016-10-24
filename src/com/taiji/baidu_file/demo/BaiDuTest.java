package com.taiji.baidu_file.demo;

/**
 * 
 * 
 */

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class BaiDuTest {

	public static void main(String[] args) throws IOException {
		
		
		//code码
		String code="mda-ge5c5nps4d7ta73g";
		
		//下载存放路径
		File file = new File("C:\\Users\\yanwei\\Desktop\\" + code + ".mp4");
		String file1=file.toString();
		
		//文件目录地址
		String src =file1;
        //上传至HDFS位置
        String dst ="/"+file.getName();
		
		/*//解析code码，获取百度http地址
		BaiduUrl bdu=new BaiduUrl();
		String url=bdu.GetBaiduUrl(code).toString();
			
		//解析http百度地址，获取下载地址
		GetUrl geturl = new GetUrl();
		String loadurl = geturl.getUrl(url);

		//开始下载文件
		DownLoadFile downloadfile=new DownLoadFile();
		downloadfile.saveUrlAs(code, loadurl, file);*/
		
		//将文件上传到HDFS
        Configuration conf = new Configuration();

		FileSystem hdfs = FileSystem.get(conf);
		Path srcPath = new Path(src);  
		Path dstPath = new Path(dst);  
		
		boolean exist=hdfs.exists(dstPath);
		 if (exist) {
			System.out.println("该文件已存在,已停止上传");
		}else {
					
		long a = System.currentTimeMillis();

		System.out.println("开始上传");

		// hdfs上传文件
		hdfs.copyFromLocalFile(srcPath, dstPath);

	   // System.out.println("Upload to"+conf.get("fs.default.name"));

		FileStatus files[] = hdfs.listStatus(dstPath);

		for (FileStatus filehdfs : files) {
			System.out.println("文件上传至:"+filehdfs.getPath());
		}
		
		long b = System.currentTimeMillis();
		
		System.out.println("上传成功,用时：" + (b - a) + "毫秒");
		
		}
	}
}


