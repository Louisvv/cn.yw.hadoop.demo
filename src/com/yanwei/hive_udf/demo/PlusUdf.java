package com.yanwei.hive_udf.demo;

import org.apache.hadoop.hive.ql.exec.UDF;

public class PlusUdf extends UDF{
	
	public Integer evaluate(Integer a, Integer b) {
		if(a==null||b==null){
			return null;
		}		
		return a-b;
	}
		
		
	}


