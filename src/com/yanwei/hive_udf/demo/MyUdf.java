package com.yanwei.hive_udf.demo;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class MyUdf extends UDF {

	public static Map<String,String> nationMap=new HashMap<String,String>();
		static	{
				nationMap.put("china", "�й�");
				nationMap.put("japan", "�ձ�");
				nationMap.put("usa", "������");
			}
		
	Text t=new Text();
	public Text evaluate(Text nation){
		String nation_e=nation.toString();
		String name = nationMap.get(nation_e);
		if(name == null){
			name="������";
		}
		t.set(name);
		return t;
	}
	
}
