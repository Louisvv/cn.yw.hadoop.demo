package com.taiji.baidu_file.demo;

/**
 * 
 * 
 */
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.baidubce.BceClientConfiguration;
import com.baidubce.auth.DefaultBceCredentials;
import com.baidubce.services.bos.BosClient;
import com.baidubce.services.bos.BosClientConfiguration;
import com.baidubce.services.vod.VodClient;
import com.baidubce.services.vod.model.GetMediaSourceDownloadResponse;

public class main{
	
	final static String code="mda-ge5cmbjgci7pyt59";
	
	public static void main(String[] args) {
		 String ACCESS_KEY_ID ="afe4759592064eee930682e399249aba";
		  String SECRET_ACCESS_KEY ="7785ea912b06449f8cbd084998a1e400";
	
			//vod类别  
		  BceClientConfiguration config =new BceClientConfiguration();
		  config.setCredentials(new DefaultBceCredentials(ACCESS_KEY_ID,SECRET_ACCESS_KEY));
		  config.setEndpoint("http://vod.baidubce.com");
		  VodClient vodClient =new VodClient(config);
		  
		  //获取视频下载地址	 
		  String url=vodClient.getMediaSourceDownload(code,-1).toString();
		 
		  //获取http下载地址
		  GetUrl gu=new GetUrl();
		  String dst=gu.getUrl(url);
		  
		  //设置文件存放路径和名称
		  File file=new File("C:\\Users\\yanwei\\Desktop\\"+code+".mp4");
		  	  
		  	 
	}
}
