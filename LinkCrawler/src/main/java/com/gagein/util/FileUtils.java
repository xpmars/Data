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
//          Jul 8, 2014     Administrator      Created
//
// CVS version control block - do not edit manually
// $Id: $

package com.gagein.util;

import java.io.File;

import org.apache.log4j.Logger;

public class FileUtils extends org.apache.commons.io.FileUtils{
	/** logger */
	private static final Logger logger = Logger.getLogger(FileUtils.class);
	/**
	 * 根据 url 和网页类型生成需要保存的网页的文件名 去除掉 url 中非文件名字符
	 */
	public static String getFileNameByUrl(String url) {
		url = url.substring(7);// remove http://
		url = url.replaceAll("[\\?/:*|<>\"]", "_") + ".html";
		return url;
	}

	public static boolean createDir(String destDirName) {
		File dir = new File(destDirName);
		if (dir.exists()) {
			logger.debug("========创建的目录" + destDirName + "========已经存在========");
			return true;
		}
		if (!destDirName.endsWith(File.separator)) {
			destDirName = destDirName + File.separator;
		}
		// 创建目录
		if (dir.mkdirs()) {
			logger.debug("========创建目录" + destDirName + "========成功！========");
			return true;
		} else {
			logger.debug("========创建目录" + destDirName + "========失败！========");
			return false;
		}
	}
}
