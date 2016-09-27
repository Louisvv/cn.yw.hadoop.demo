package org.taiji.hive.udf.demo;

import org.apache.hadoop.hive.ql.exec.UDF;

public class HelloUdf extends UDF{
	public String evaluate(String str){
		try {
			return "Hello" +str;
		} catch (Exception e) {
			return null;
		}
	}
	

}
