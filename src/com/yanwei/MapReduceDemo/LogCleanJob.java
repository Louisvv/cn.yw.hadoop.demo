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


public class LogCleanJob extends Configured implements Tool {

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        try {
            int res = ToolRunner.run(conf, new LogCleanJob(), args);
            System.exit(res);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    
    
    @Override
    public int run(String[] args) throws Exception {
        final Job job = new Job(new Configuration(),
                LogCleanJob.class.getSimpleName());
        
        // ����Ϊ���Դ������
        job.setJarByClass(LogCleanJob.class);
        
        FileInputFormat.setInputPaths(job, args[0]);
        
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        // �����Ѵ��ڵ�����ļ�
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

            
            
            // step1.���˵���̬��Դ��������
            if (parsed[2].startsWith("GET /static/")
                    || parsed[2].startsWith("GET /uc_server")) {
                return;
            }
            // step2.���˵���ͷ��ָ���ַ���
            if (parsed[2].startsWith("GET /")) {
                parsed[2] = parsed[2].substring("GET /".length());
            } else if (parsed[2].startsWith("POST /")) {
                parsed[2] = parsed[2].substring("POST /".length());
            }
            // step3.���˵���β���ض��ַ���
            if (parsed[2].endsWith(" HTTP/1.1")) {
                parsed[2] = parsed[2].substring(0, parsed[2].length()
                        - " HTTP/1.1".length());
            }
            // step4.ֻд��ǰ������¼������
            outputValue.set(parsed[0] + "\t" + parsed[2] + "\t" + parsed[1]);
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
    }

    /*
     * ��־������
     */
    static class LogParser {
        public static final SimpleDateFormat FORMAT = new SimpleDateFormat(
                "d/MMM/yyyy:HH:mm:ss", Locale.ENGLISH);
        public static final SimpleDateFormat dateformat1 = new SimpleDateFormat(
                "yyyyMMddHHmmss");

        public static void main(String[] args) throws ParseException {
            final String S1 = "27.19.74.143 - - [30/May/2013:17:38:20 +0800] \"GET /static/image/common/faq.gif HTTP/1.1\" 200 1127";
            LogParser parser = new LogParser();
            final String[] array = parser.parse(S1);
            System.out.println("�������ݣ� " + S1);
            System.out.format(
                    "���������  ip=%s, time=%s, url=%s, status=%s, traffic=%s",
                    array[0], array[1], array[2], array[3], array[4]);
        }

        /**
         * ����Ӣ��ʱ���ַ���
         * 
         * @param string
         * @return
         * @throws ParseException
         */
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
         * @return ���麬��5��Ԫ�أ��ֱ���ip��ʱ�䡢url��״̬������
         */
        public String[] parse(String line) {
            String ip = parseIP(line);
            String time = parseTime(line);
            String url = parseURL(line);
            String status = parseStatus(line);
            String traffic = parseTraffic(line);

            return new String[] { ip, time, url, status, traffic };
        }

        private String parseTraffic(String line) {
            final String trim = line.substring(line.lastIndexOf("\"") + 1)
                    .trim();
            String traffic = trim.split(" ")[1];
            return traffic;
        }

        private String parseStatus(String line) {
            final String trim = line.substring(line.lastIndexOf("\"") + 1)
                    .trim();
            String status = trim.split(" ")[0];
            return status;
        }

        private String parseURL(String line) {
            final int first = line.indexOf("\"");
            final int last = line.lastIndexOf("\"");
            String url = line.substring(first + 1, last);
            return url;
        }

        private String parseTime(String line) {
            final int first = line.indexOf("[");
            final int last = line.indexOf("+0800]");
            String time = line.substring(first + 1, last).trim();
            Date date = parseDateFormat(time);
            return dateformat1.format(date);
        }

        private String parseIP(String line) {
            String ip = line.split("- -")[0].trim();
            return ip;
        }
    }
}