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
//          Jul 14, 2014     Administrator      Created
//
// CVS version control block - do not edit manually
// $Id: $

package com.gagein.util;

import java.io.File;

import com.gagein.crawler.Crawler;

public class UrlAndPathUtils {
	//去掉url的'http://'前缀
	public static String RemoveHttpPrefix(String url){
		return url.substring(7);
	}
	//然后取得域名
		public static String getDomain(String url){
			return RemoveHttpPrefix(url).split("\\.")[1];
		}
	//得到下载主页的路径
	public static String getMainUrlPath(Crawler crawler, String fileName){
		return crawler.getDirPath() + File.separator + fileName;
	}
	
}
