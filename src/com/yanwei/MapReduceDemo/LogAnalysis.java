package com.yanwei.MapReduceDemo;


import java.lang.reflect.Array;
import java.sql.Time;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;



/*
 * 日志处理程序
 * 2016/9/19
 * 闫威
 * 处理一时一刻网站日志
 */

public class LogAnalysis {

	
	public void TimePlus(String[] time){
		System.out.println(Array.get(time, 2));
		
	}
	
	public static void main(String[] args) throws ParseException {
		

		/**
		 * 解析英文时间字符串
		 * 
		 * @param string
		 * @return
		 * @throws ParseException
		 */

		final String S1 = "1008,1008,[30/May/2013:21:00:00 +0800]";
			
		
		
		LogAnalysis analysis = new LogAnalysis();
		final String[] array = analysis.parse(S1);
		analysis.TimePlus(array);
		
		System.out.println("样例数据： " + S1);
		System.out.format(
				"解析结果：  id=%s, user=%s, time=%s",
				array[0], array[1], array[2]);
	}

		
	


	/**
	 * 解析英文时间字符串
	 * 
	 * @param string
	 * @return
	 * @throws ParseException
	 */
	
	
	public static final SimpleDateFormat FORMAT = new SimpleDateFormat(
			"d/MMM/yyyy:HH:mm:ss", Locale.ENGLISH);
	public static final SimpleDateFormat dateformat1 = new SimpleDateFormat(
			"yyyyMMddHHmmss");

	private Date parseDateFormat(String string) {
		Date parse = null;
		try {
			parse = FORMAT.parse(string);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return parse;
	}

	
	/**
	 * 解析日志的行记录
	 * 
	 * @param line
	 * @return 数组含有3个元素，分别是老师、课、时间
	 *  
	 */
	
	
	public String[] parse(String line) {
		String id = parseID(line);
		String time = parseTime(line);
		String user = parseUser(line);

		return new String[] { id, user, time };
	}

	

	private String parseUser(String line) {
		String user = line.split(",")[1].trim();
		return user;
	}

	private String parseTime(String line) {
		final int first = line.indexOf("[");
		final int last = line.indexOf("+0800]");
		String time = line.substring(first + 1, last).trim();
		//时间转换	
		Date date = parseDateFormat(time);
		return dateformat1.format(date);		
	}

	private String parseID(String line) {
		String id = line.split(",")[0].trim();
		return id;
	}
}





