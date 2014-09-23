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

package com.gagein.crawler;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import com.gagein.dp.ws.textstrip.IArticleStrip;
import com.gagein.dp.ws.textstrip.boilerpipe.BoilerpipeStripper;
import com.gagein.dp.ws.textstrip.gagi.consolidate.Article;
import com.gagein.dp.ws.textstrip.gagi.consolidate.ArticleStripper;

public class Extractor {
	/** logger */
	private static final Logger logger = Logger.getLogger(Crawler.class);
	private Set<String> pathSet;

	public void extact(String dirPath) {
		logger.debug("============开始提取文本操作");
		getFilesPath(dirPath);
		System.out.println(pathSet);
		ArticleStripper as = new ArticleStripper();
		as.setExtractAuthor(false);
		as.setExtractContent(true);
		as.setExtractDate(false);
		as.setExtractImg(false);
		as.setKeepContentLink(false);

		for (String url : pathSet) {
			System.out.println(url);
			File file = new File(url);
			try {
				logger.debug("==============开始读取" + url + "文本！");
				String page = FileUtils.readFileToString(file);

				IArticleStrip bbs = new BoilerpipeStripper();
				String content = bbs.stripToCommonTextArticle(null, null, page);
				System.out.println("url：##################" + url);
				System.out.println("Content：##################" + content);

			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) {
		String website = "http://www.gagein.com";
		String tempName = "temp";
		// 下载网页之前，生成对应公司目录
		String dirPath = System.getProperty("user.dir") + File.separator + tempName + File.separator
				+ website.substring(7).split("\\.")[1];// remove http://;
		Extractor ex = new Extractor();
		ex.getFilesPath(dirPath);
		System.out.println(ex.pathSet);
		ArticleStripper as = new ArticleStripper();
		as.setExtractAuthor(false);
		as.setExtractContent(true);
		as.setExtractDate(false);
		as.setExtractImg(false);
		as.setKeepContentLink(false);
		for (String url : ex.pathSet) {
			System.out.println(url);
			File file = new File(url);
			try {
				String page = FileUtils.readFileToString(file);
				System.out.println("Contents of file: " + page);
				logger.debug("==============开始读取" + url + "文本！");
				Article article = as.retrieveArticle(null, null, page, null);
				System.out.println("##################" + article.getContent());

			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

	// 取得下载网页的本地地址（path + url）
	public void getFilesPath(String dirPath) {
		pathSet = new HashSet<String>();
		File dir = new File(dirPath);
		if (!dir.exists() || !dir.isDirectory())
			System.out.println("路径不存在");
		else {
			System.out.println("该路径文件列表如下:");
			System.out.println(dir.getPath());
		}
		File[] dirs = dir.listFiles();
		if (dir != null) {
			for (File f : dirs) {
				if (!f.isDirectory()) {
					pathSet.add(f.getAbsolutePath());
				}
			}
		}

	}

}
