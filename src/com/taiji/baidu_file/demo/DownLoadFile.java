package com.taiji.baidu_file.demo;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

/** 
 * @author 闫威 
 * 下载获取到URL地址的文件，将文件保存到本地中
 * @version 2016年10月24日 上午11:05:01
 */
public class DownLoadFile {
	public void saveUrlAs(String code,String Url, File fileName){
        //此方法只能用HTTP协议
        //保存文件到本地
        //Url是文件下载地址,fileName 为一个全名(路径+文件名)文件
        URL url;
        DataOutputStream out = null;
        DataInputStream in = null;
      
        try {
            url = new URL(Url);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            in = new DataInputStream(connection.getInputStream());
            out = new DataOutputStream(new FileOutputStream(fileName));
            byte[] buffer = new byte[67108864];
            int count = 0;
            long a=0l;
            long b=0l;
            System.out.println("开始下载文件:"+code);
            a=System.currentTimeMillis();
                 while ((count = in.read(buffer)) > 0) {
                     out.write(buffer, 0, count);
                 }
                 b=System.currentTimeMillis();
                 System.out.println("下载结束，下载用时："+(b-a)/1000+"秒");
                 
        }catch (Exception e) {
            e.printStackTrace();
        }finally{
            try {
                if(out != null){
                    out.close();
                }
                if(in != null){
                    in.close();
                }
            }       
            catch (IOException e) {
                e.printStackTrace();
            }
        }
       
    }
}
