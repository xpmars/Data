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
//          Jul 7, 2014     Administrator      Created
//
// CVS version control block - do not edit manually
// $Id: $

package com.gagein.crawler;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Set;

import org.apache.log4j.Logger;
import org.htmlparser.util.ParserException;

//去重
public class RmDuplicated {
	/** logger */
	private static final Logger logger = Logger.getLogger(RmDuplicated.class);

	public void rmDup(String dirPath) {
		logger.debug("============开始去重操作");
		Properties prop = new Properties();
		InputStream in = RmDuplicated.class.getResourceAsStream("/props.properties");
		try {
			prop.load(in);
		} catch (IOException e) {
			e.printStackTrace();
		}
		Set<Long> sizeSet = new HashSet<Long>();
		System.out.println("#################去重路径：" + dirPath);
		LinkedList<File> list = new LinkedList<File>();
		File dir = new File(dirPath);
		File file[] = dir.listFiles();
		if (file != null) {
			for (int i = 0; i < file.length; i++) {
				// 去重仅仅比较文件大小，需要优化
				if (sizeSet.contains(file[i].length())) {
					logger.debug(file[i] + "文件内容重复，开始去重操作，将其删除");
					file[i].delete();
					continue;
				} else {
					sizeSet.add(file[i].length());
					list.add(file[i]);
				}
				System.out.println("大小：" + file[i].length() + "===" + file[i].getAbsolutePath());
			}
		}
		// 以响应页面大小来判定相似页面,如有大小一样的
		System.out.println(list);
	}

	/**
	 * 删除单个文件
	 * 
	 * @param sPath
	 *            被删除文件的文件名
	 * @return 单个文件删除成功返回true，否则返回false
	 */
	public boolean deleteFile(String sPath) {
		boolean flag = false;
		File file = new File(sPath);
		// 路径为文件且不为空则进行删除
		if (file.isFile() && file.exists()) {
			file.delete();
			flag = true;
		}
		return flag;
	}

	public static void main(String[] args) throws ParserException {
		String filePath = System.getProperty("user.dir") + File.separator + "temp" + File.separator;
		Properties prop = new Properties();
		InputStream in = RmDuplicated.class.getResourceAsStream("/props.properties");
		try {
			prop.load(in);
			filePath = System.getProperty("user.dir") + prop.getProperty("temp.path");
		} catch (IOException e) {
			e.printStackTrace();
		}
		Set<Long> sizeSet = new HashSet<Long>();
		System.out.println(filePath);
		LinkedList<File> list = new LinkedList<File>();
		File dir = new File(filePath);
		File file[] = dir.listFiles();
		for (int i = 0; i < file.length; i++) {
			// 去重仅仅比较文件大小，需要优化
			if (sizeSet.contains(file[i].length())) {
				file[i].delete();
				continue;
			} else {
				sizeSet.add(file[i].length());
				list.add(file[i]);
			}
			System.out.println("大小：" + file[i].length() + "===" + file[i].getAbsolutePath());
		}

		// 以响应页面大小来判定相似页面,如有大小一样的
		System.out.println(list);

	}
}
