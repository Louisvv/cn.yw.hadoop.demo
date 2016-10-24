package com.taiji.baidu_file.demo;

import com.baidubce.BceClientConfiguration;
import com.baidubce.auth.DefaultBceCredentials;
import com.baidubce.services.bos.BosClient;
import com.baidubce.services.bos.BosClientConfiguration;
import com.baidubce.services.vod.VodClient;
import com.baidubce.services.vod.model.GetMediaSourceDownloadResponse;

/**
 * @author 闫威
 * @version 2016年10月24日 上午11:17:38
 */
public class BaiduUrl {
	
	public String GetBaiduUrl(String code) {
	String ACCESS_KEY_ID = "afe4759592064eee930682e399249aba";
	String SECRET_ACCESS_KEY = "7785ea912b06449f8cbd084998a1e400";
	// vod类别
	
		BceClientConfiguration config = new BceClientConfiguration();
		config.setCredentials(new DefaultBceCredentials(ACCESS_KEY_ID,
				SECRET_ACCESS_KEY));
		config.setEndpoint("http://vod.baidubce.com");
		VodClient vodClient = new VodClient(config);

		// 获取视频下载地址
		String url = vodClient.getMediaSourceDownload(code, -1).toString();
		System.out.println("已获取文件下载url");
		
		return url;
		//System.out.println(url);
	}
}
