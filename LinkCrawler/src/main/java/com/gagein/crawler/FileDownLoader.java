//
//                               GageIn, Inc.
//                     Confidential and Proprietary
//                         ALL RIGHTS RESERVED.
//
//      This software is provided under license and may be used
//      or distributed only in accordance with the terms of
//      such license.
//
//
// History: Date        By whom      what
//          Jul 4, 2014     Administrator      Created
//
// CVS version control block - do not edit manually
// $Id: $

package com.gagein.crawler;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.log4j.Logger;

import com.gagein.bean.HtmlFileBean;
import com.gagein.dp.platform.common.api.util.HttpUtil;
import com.gagein.dp.platform.common.api.util.HttpUtil.HttpUtilResponse;
import com.gagein.util.FileUtils;

public class FileDownLoader {
	/** logger */
	private static final Logger logger = Logger.getLogger(FileDownLoader.class);

	/**
	 * 保存网页字节数组到本地文件 filePath 为要保存的文件的相对地址
	 */
	private void saveToLocal(byte[] data, String filePath) {
		try {
			System.out.println("========SaveToLocal========filePath===" + filePath);
			DataOutputStream out = new DataOutputStream(new FileOutputStream(new File(filePath)));
			for (int i = 0; i < data.length; i++)
				out.write(data[i]);
			out.flush();
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/* 下载 公司主页 ,返回编码格式 */
	public void downloadMainPage(HtmlFileBean hfb) {
		String charset = null;
		try {
			HttpUtilResponse res = HttpUtil.retrievePageInfo(hfb.getUrl());
			if (res != null) {
				FileUtils.writeByteArrayToFile(new File(hfb.getMainFilePath()), res.getContent());
				charset = res.getCharset();
				hfb.setCharset(charset);
			} else {
				System.out.println("on");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

	/* 下载 url 指向的网页 */
	public String downloadLink(String url, HtmlFileBean hfb) {
		/* 1.生成 HttpClinet 对象并设置参数 */
		HttpClient httpClient = new HttpClient();
		// 设置 Http 连接超时 5s
		httpClient.getHttpConnectionManager().getParams().setConnectionTimeout(5000);
		/* 2.生成 GetMethod 对象并设置参数 */
		GetMethod getMethod = new GetMethod(url);
		// 设置 get 请求超时 5s
		getMethod.getParams().setParameter(HttpMethodParams.SO_TIMEOUT, 5000);
		// 设置请求重试处理
		getMethod.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler());

		/* 3.执行 HTTP GET 请求 */
		try {
			int statusCode = httpClient.executeMethod(getMethod);
			if (statusCode != HttpStatus.SC_OK) {
				System.err.println("Method failed: " + getMethod.getStatusLine());
			}
			/* 4.处理 HTTP 响应内容 */
			byte[] responseBody = getMethod.getResponseBody();// 读取为字节数组

			// 根据网页 url 生成保存时的文件名
			url = FileUtils.getFileNameByUrl(url);

			saveToLocal(responseBody, hfb.getDirPath() + File.separator + url);
		} catch (HttpException e) {
			// 发生致命的异常，可能是协议不对或者返回的内容有问题
			logger.debug("Please check your provided http " + "address!");
			e.printStackTrace();
		} catch (IOException e) {
			// 发生网络异常
			logger.debug("网络异常！");
			e.printStackTrace();
		} finally {
			// 释放连接
			logger.debug("==============释放=========" + url + "=============链接！");
			getMethod.releaseConnection();
		}
		return hfb.getDirPath() + url;
	}

	// 测试的 main 方法
	public static void main(String[] args) {
		
	}
}