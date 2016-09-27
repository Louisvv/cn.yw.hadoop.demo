package com.yanwei.MapReduceDemo;

import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class LogCleanYw extends Configured implements Tool {

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        try {
            int res = ToolRunner.run(conf, new LogCleanYw(), args);
            System.exit(res);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    
    
    @Override
    public int run(String[] args) throws Exception {
        final Job job = new Job(new Configuration(),
                LogCleanYw.class.getSimpleName());
        
        // 设置为可以打包运行
        job.setJarByClass(LogCleanYw.class);
        
        FileInputFormat.setInputPaths(job, args[0]);
        
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        // 清理已存在的输出文件
        FileSystem fs = FileSystem.get(new URI(args[0]), getConf());
        Path outPath = new Path(args[1]);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }
        
        boolean success = job.waitForCompletion(true);
        if(success){
            System.out.println("Clean process success!");
        }
        else{
            System.out.println("Clean process failed!");
        }
        return 0;
    }
    
    
    
    

    static class MyMapper extends
            Mapper<LongWritable, Text, LongWritable, Text> {
        LogParser logParser = new LogParser();
        Text outputValue = new Text();

        protected void map(
                LongWritable key,
                Text value,
                org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, LongWritable, Text>.Context context)
                throws java.io.IOException, InterruptedException {
            final String[] parsed = logParser.parse(value.toString());
            outputValue.set(parsed[0] + "\t" + parsed[1] + "\t" + parsed[2]);
            context.write(key, outputValue);
        }
    }

    static class MyReducer extends
            Reducer<LongWritable, Text, Text, NullWritable> {
        protected void reduce(
                LongWritable k2,
                java.lang.Iterable<Text> v2s,
                org.apache.hadoop.mapreduce.Reducer<LongWritable, Text, Text, NullWritable>.Context context)
                throws java.io.IOException, InterruptedException {
            for (Text v2 : v2s) {
                context.write(v2, NullWritable.get());
            }
        };

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
}
