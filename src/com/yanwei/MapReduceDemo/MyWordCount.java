package com.yanwei.MapReduceDemo;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        
public class MyWordCount {
  
  
  /**
   * Map: ��������ı�����ת��Ϊ<word-1>�ļ�ֵ��
   * */
  public static class WordCountMap extends Mapper<LongWritable, Text, Text, IntWritable> {
    
    String regex = "[.,\"!--;:?'\\]]"; //remove all punctuation
    Text word = new Text();
    final static IntWritable one = new IntWritable(1);
    HashSet<String> stopWordSet = new HashSet<String>();
    
    /**
     * ��ͣ�ʴ��ļ�����hashSet��
     * */
    
    private void parseStopWordFile(String path){
      try {
        String word = null;
        BufferedReader reader = new BufferedReader(new FileReader(path));
        while((word = reader.readLine()) != null){
          stopWordSet.add(word);
        }
      } catch (IOException e) {
        e.printStackTrace();
      }	
    }
    
    /**
     * ���map��ʼ������
     * ��Ҫ�Ƕ�ȡͣ���ļ�
     * */
    
    public void setup(Context context) {			
      
      Path[] patternsFiles = new Path[0];
      try {
        patternsFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
      } catch (IOException e) {
        e.printStackTrace();
      }			
      if(patternsFiles == null){
        System.out.println("have no stopfile\n");
        return;
      }
      
      //read stop-words into HashSet
      for (Path patternsFile : patternsFiles) {
        parseStopWordFile(patternsFile.toString());
      }
    }  
    
    /**
     *  map
     * */
    public void map(LongWritable key, Text value, Context context) 
      throws IOException, InterruptedException {
      
      String s = null;
      String line = value.toString().toLowerCase();
      line = line.replaceAll(regex, " "); //remove all punctuation
      
      //split all words of line
      StringTokenizer tokenizer = new StringTokenizer(line);
      while (tokenizer.hasMoreTokens()) {
        s = tokenizer.nextToken();
        if(!stopWordSet.contains(s)){
          word.set(s);
          context.write(word, one);
        }				
      }
    }
  }
  
  /**
   * Reduce: add all word-counts for a key
   * */
  public static class WordCountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    
    int min_num = 0;
    
    /**
     * minimum showing words
     * */
    public void setup(Context context) {
      min_num = Integer.parseInt(context.getConfiguration().get("min_num"));
      System.out.println(min_num);
    }
    
    /**
     * reduce
     * */
    public void reduce(Text key, Iterable<IntWritable> values, Context context)	
      throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      if(sum < min_num) return;
      context.write(key, new IntWritable(sum));
    }
  }
  
  /**
   * IntWritable comparator
   * */
  private static class IntWritableDecreasingComparator extends IntWritable.Comparator {
        
        public int compare(WritableComparable a, WritableComparable b) {
      	  return -super.compare(a, b);
        }
        
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
  }
  
  /**
   * main: run two job
   * */
  public static void main(String[] args){
    
    boolean exit = false;
    String skipfile = null; //stop-file path
    int min_num = 0;
    String tempDir = "wordcount-temp-" + Integer.toString(new Random().nextInt(Integer.MAX_VALUE));
    
    Configuration conf = new Configuration();
    
    //��ȡͣ���ļ���·�������ŵ�DistributedCache��
      for(int i=0;i<args.length;i++)
      {
      if("-skip".equals(args[i]))
      {
        DistributedCache.addCacheFile(new Path(args[++i]).toUri(), conf);
        System.out.println(args[i]);
      }			
    }
     
      
      //��ȡҪչʾ����С��Ƶ
      for(int i=0;i<args.length;i++)
      {
      if("-greater".equals(args[i])){
        min_num = Integer.parseInt(args[++i]);
        System.out.println(args[i]);
      }			
    }
      
    //����С��Ƶֵ�ŵ�Configuration�й���
    conf.set("min_num", String.valueOf(min_num));	//set global parameter
    
    try{
      /**
       * run first-round to count
       * */
      Job job = new Job(conf, "���ݷ����׶�-1");
      job.setJarByClass(MyWordCount.class);
      
      //set format of input-output
      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(SequenceFileOutputFormat.class);
      
      //set class of output's key-value of MAP
      job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        //set mapper and reducer
        job.setMapperClass(WordCountMap.class);     
        job.setReducerClass(WordCountReduce.class);
        
        //set path of input-output
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(tempDir));
        
        
        
        if(job.waitForCompletion(true)){		    
          /**
           * run two-round to sort
           * */
          //Configuration conf2 = new Configuration();
        Job job2 = new Job(conf, "���ݷ����׶�-2");
        job2.setJarByClass(MyWordCount.class);
        
        //set format of input-output
        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);		
        
        //set class of output's key-value
        job2.setOutputKeyClass(IntWritable.class);
          job2.setOutputValueClass(Text.class);
          
          //set mapper and reducer
          //InverseMapper������ʵ��map()֮������ݶԵ�key��value����
          //��Reducer�ĸ����޶�Ϊ1, ��������Ľ���ļ�����һ��
        /**
        * ע�⣬���ｫreduce����Ŀ����Ϊ1�����кܴ�����¡�
        * ��Ϊhadoop�޷����м���ȫ������ֻ����һ��reduce�ڲ�
        * �ı������� ��������Ҫ����һ�����ռ���ȫ�ֵ�����
        * ��ֱ�ӵķ�����������reduceֻ��һ����
        */
          job2.setMapperClass(InverseMapper.class);    
          job2.setNumReduceTasks(1); 
          
          //set path of input-output
          FileInputFormat.addInputPath(job2, new Path(tempDir));
          FileOutputFormat.setOutputPath(job2, new Path(args[1]));
          
          /**
           * Hadoop Ĭ�϶� IntWritable ���������򣬶�������Ҫ���ǰ��������С�
           * �������ʵ����һ�� IntWritableDecreasingComparator ��,��
         * ��ָ��ʹ������Զ���� Comparator ����������е� key (��Ƶ)��������
           * */
          job2.setSortComparatorClass(IntWritableDecreasingComparator.class);
          exit = job2.waitForCompletion(true);
        }
    }catch(Exception e){
      e.printStackTrace();
    }finally{
        
        try {
        	//delete tempt dir
        FileSystem.get(conf).deleteOnExit(new Path(tempDir));
        if(exit) System.exit(1);
        System.exit(0);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
         
 }