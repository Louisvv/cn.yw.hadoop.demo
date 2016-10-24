package com.taiji.baidu_file.demo;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** 
 * @author 闫威
 * 通过解析的code码，获取下载资源的URL地址
 * @version 2016年10月24日 上午11:02:25
 */
public class GetUrl {
	public String getUrl(String Url) {
		 String regex="([a-zA-z]+://[^\\s]*)";
		 Pattern pattern = Pattern.compile(regex);
		 Matcher mt=pattern.matcher(Url);
		 if(mt.find()){
			 String dst=mt.group(1);
			//System.out.println(dst);
		 }
		String url=mt.group(1);
		System.out.println("正在解析url");
		return url;
	}
}
