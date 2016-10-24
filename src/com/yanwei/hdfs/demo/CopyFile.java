package com.yanwei.hdfs.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
 
public class CopyFile {
    public static void main(String[] args) throws Exception {
        Configuration conf=new Configuration();
        FileSystem hdfs=FileSystem.get(conf);
        
//        FileSystem hdfs1=FileSystem.get(uri, conf, user)
       
        //�����ļ�
        Path src =new Path("C:\\Users\\yanwei\\Desktop\\1.jpg");
        //HDFSλ��
        Path dst =new Path("/M01");
       
        hdfs.copyFromLocalFile(src, dst);
        //hdfs.copyFromLocalFile(src, dst);
        System.out.println("Upload to"+conf.get("fs.default.name"));
       
        FileStatus files[]=hdfs.listStatus(dst);
        for(FileStatus file:files){
            System.out.println(file.getPath());
        }
    }
}