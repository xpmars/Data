package com.gagein.crawler;

import java.io.File;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.gagein.structure.LinkDB;
import org.apache.log4j.Logger;
import org.jsoup.helper.StringUtil;

import com.gagein.bean.HtmlFileBean;
import com.gagein.thread.DownloadTask;
import com.gagein.util.FileUtils;
import com.gagein.util.UrlAndPathUtils;

/**
 * 爬虫主程序
 * 
 */
public class Crawler {
	/** logger */
	private static final Logger logger = Logger.getLogger(Crawler.class);
	// 务必加上http://
	// private static final String website = "http://www.gagein.com";
	private String website = "http://www.baidu.com";
	private String keyword = "about";
	private String tempName = "temp";
	private LinkDB linkDB = new LinkDB();
	private String dirPath;

	/* 再从LinkDB中获取要进一步爬取得链接，只爬去一层链接 */
	public void crawling() {
		// ----1.初始化
		HtmlFileBean hfb = new HtmlFileBean();
		init(hfb);
		// -----2.下载主页
		new FileDownLoader().downloadMainPage(hfb);

		// -----3.从已下载的主页中提取链接准备下载,采用HtmlParser分析并提取
		logger.debug("==============开始抽取包含关键字：" + keyword + "链接");
		Set<String> links = HtmlParserTool.extracLinks(hfb);

		// -----4.下载,先构造线程池,为每一个待下载链接分配一个线程
		ExecutorService pool = Executors.newCachedThreadPool();
		pool = Executors.newCachedThreadPool();
		logger.debug("==============开始抽取包含关键字：" + keyword + "链接");
		// 新的未访问的 URL 入队
		int count = 1;
		for (String link : links) {
			if (!StringUtil.isBlank(link)) {
				logger.debug("==============第" + count++ + "次，获取含有关键字的链接：" + link + ",并下载！");
				DownloadTask t = new DownloadTask(link, hfb);
				pool.execute(t);
				linkDB.addVisitedUrl(link);
			}
		}

		// 线程池中线程执行完毕
		while (true) {
			if (DownloadTask.count <= 0) {
				logger.debug("==============线程执行结束了,关闭线程池！");
				pool.shutdown();
				break;
			}
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		logger.debug("==============抓取结束");

	}

	/**
	 * 初始化方法
	 */
	private void init(HtmlFileBean hfb) {

		// -----下载网页之前，生成对应公司目录
		setDirPath(website);
		if (FileUtils.createDir(dirPath) == false) {
			logger.error("========生成目录失败！========");
			return;
		}

		// -----根据网页 url 生成保存时的文件名 mainUrl
		String fileName = FileUtils.getFileNameByUrl(website);
		String mainFilePath = UrlAndPathUtils.getMainUrlPath(this, fileName);
		hfb.setUrl(website);
		hfb.setDomain(UrlAndPathUtils.getDomain(website));
		hfb.setTempName(tempName);
		hfb.setDirPath(dirPath);
		hfb.setFileName(fileName);
		hfb.setMainFilePath(mainFilePath);
		hfb.setKeyword(keyword);

	}

	public static void main(String[] args) {
		long a = System.currentTimeMillis();
		Crawler crawler = new Crawler();
		crawler.crawling();
		System.out.println("\r<br>执行耗时 : " + (System.currentTimeMillis() - a) / 1000f + " 秒 ");
	}

	public String getWebsite() {
		return website;
	}

	public void setWebsite(String website) {
		this.website = website;
	}

	public String getKeyword() {
		return keyword;
	}

	public void setKeyword(String keyword) {
		this.keyword = keyword;
	}

	public String getTempName() {
		return tempName;
	}

	public void setTempName(String tempName) {
		this.tempName = tempName;
	}

	public LinkDB getLinkDB() {
		return linkDB;
	}

	public void setLinkDB(LinkDB linkDB) {
		this.linkDB = linkDB;
	}

	public String getDirPath() {
		return dirPath;
	}

	public void setDirPath(String website) {
		this.dirPath = System.getProperty("user.dir") + File.separator + this.tempName + File.separator
				+ UrlAndPathUtils.getDomain(website);
	}

}
