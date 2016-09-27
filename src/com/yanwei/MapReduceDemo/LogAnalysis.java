package com.yanwei.MapReduceDemo;


import java.lang.reflect.Array;
import java.sql.Time;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;



/*
 * ��־�������
 * 2016/9/19
 * ����
 * ����һʱһ����վ��־
 */

public class LogAnalysis {

	
	public void TimePlus(String[] time){
		System.out.println(Array.get(time, 2));
		
	}
	
	public static void main(String[] args) throws ParseException {
		

		/**
		 * ����Ӣ��ʱ���ַ���
		 * 
		 * @param string
		 * @return
		 * @throws ParseException
		 */

		final String S1 = "1008,1008,[30/May/2013:21:00:00 +0800]";
			
		
		
		LogAnalysis analysis = new LogAnalysis();
		final String[] array = analysis.parse(S1);
		analysis.TimePlus(array);
		
		System.out.println("�������ݣ� " + S1);
		System.out.format(
				"���������  id=%s, user=%s, time=%s",
				array[0], array[1], array[2]);
	}

		
	


	/**
	 * ����Ӣ��ʱ���ַ���
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
	 * ������־���м�¼
	 * 
	 * @param line
	 * @return ���麬��3��Ԫ�أ��ֱ�����ʦ���Ρ�ʱ��
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
		//ʱ��ת��	
		Date date = parseDateFormat(time);
		return dateformat1.format(date);		
	}

	private String parseID(String line) {
		String id = line.split(",")[0].trim();
		return id;
	}
}





