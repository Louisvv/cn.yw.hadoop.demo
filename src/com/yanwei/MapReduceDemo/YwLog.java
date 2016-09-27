package com.yanwei.MapReduceDemo;



import java.io.IOException;
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

	public class YwLog { 	    
		    
	public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer tk = new StringTokenizer(line);
			while (tk.hasMoreElements()) {
				String strTeacherName = tk.nextToken();
				String strClassName = tk.nextToken();
				String strTime = tk.nextToken();
				context.write(new Text(strTeacherName + ":" + strClassName),
						new IntWritable(Integer.parseInt(strTime)));
				// context.write(new Text(strTeacherName), new
				// IntWritable(Integer.parseInt(strTime)));
			}

		}
	}

		public static class MyReducer extends

				Reducer<Text, IntWritable, Text, IntWritable> {

			@Override
			protected void reduce(Text key, Iterable<IntWritable> values,
					Context context) throws IOException, InterruptedException {
				int sum = 0;
				int num = 0;
				for (IntWritable time : values) {
					sum += time.get();
					num++;
				}
				context.write(key, new IntWritable((int) (sum)));

			}

		}
		
		public static void main(String[] args) throws Exception {
			Configuration conf = new Configuration();//获得Configuration配置 Configuration: core-default.xml, core-site.xml    
			String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();//获得输入参数 [hdfs://localhost:9000/user/dat/input, hdfs://localhost:9000/user/dat/output]    
	        if(otherArgs.length != 2){//判断输入参数个数，不为两个异常退出    
	            System.err.println("Usage:<in> <out>");    
	            System.exit(2);    
	        }    
	        Job job = new Job(conf,"log analysis");    
	        
	        job.setJarByClass(YwLog.class);
	        
	        job.setOutputKeyClass(Text.class); 
	        job.setOutputValueClass(IntWritable.class); 
	        
	        job.setMapperClass(MyMapper.class); 
	       // job.setCombinerClass(MyReducer.class); 
	        job.setReducerClass(MyReducer.class); 
	        
	        job.setInputFormatClass(TextInputFormat.class); 
	        job.setOutputFormatClass(TextOutputFormat.class); 
	        
	        FileInputFormat.setInputPaths(job, new Path(otherArgs[0])); 
	        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1])); 
	        
	        
	        boolean success = job.waitForCompletion(true); 
	      //  System.exit(job.waitForCompletion(true)?0:1);
		
		} 
	}
	


