package com.yanwei.MapReduceDemo;


import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.tools.GetConf;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/*
 * 
 * 
 */

public class Teacher_log_parser {

	public static class MyMapper extends
			Mapper<LongWritable, Text, Text, LongWritable> {

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer tk = new StringTokenizer(line);
			while (tk.hasMoreElements()) {
				String strTeacherName = tk.nextToken();
				String strClassName = tk.nextToken();
				String strTime = tk.nextToken();
				context.write(new Text(strTeacherName + "\t" + strClassName),
						new LongWritable(Long.parseLong(strTime)));
			}
		}
	}

	public static class MyReducer extends

	Reducer<Text, LongWritable, Text, IntWritable> {

		@Override
		protected void reduce(Text key, Iterable<LongWritable> values,
				Context context) throws IOException, InterruptedException {
			int b = 0;
			List<Long> al = new ArrayList<Long>();
			for (LongWritable time : values) {
				al.add((long) time.get());
				}
				Collections.sort(al);
				for (int i = 1;i<al.size(); i++) {
						b = (int)(b+(al.get(i)-al.get(i-1)));
			}
			context.write(key, new IntWritable((int) (b)));
		}
	}

	static class LongDecreasingComparator extends LongWritable.Comparator {

		public int compare(WritableComparable a, WritableComparable b) {
			return -super.compare(a, b);
		}

		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return -super.compare(b1, s1, l1, b2, s2, l2);
		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();// 获得Configuration配置
													// Configuration:
													// core-default.xml,
													// core-site.xml
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();// 获得输入参数

		if (otherArgs.length != 2) {// 判断输入参数个数，不为两个异常退出
			System.err.println("Usage:wordcount <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "log analysis");

		job.setJarByClass(Teacher_log_parser.class);

		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);

		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// job.setOutputKeyClass(Text.class);
		// job.setOutputValueClass(LongWritable.class);

		// job.setMapperClass(MyMapper.class);
		// job.setCombinerClass(MyReducer.class);
		// job.setReducerClass(MyReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		//job.setSortComparatorClass(LongDecreasingComparator.class);

		// boolean success = job.waitForCompletion(true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
