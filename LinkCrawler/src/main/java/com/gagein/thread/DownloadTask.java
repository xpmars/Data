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

package com.gagein.thread;

import com.gagein.bean.HtmlFileBean;
import com.gagein.crawler.FileDownLoader;

public class DownloadTask implements Runnable {
	private String link;
	private HtmlFileBean hfb;
	public static int count = 0;
	public DownloadTask(String link, HtmlFileBean hfb) {
		this.link = link;
		this.hfb = hfb;
	}

	@Override
	public void run() {
		count++;
		System.out.println("\n=========下载页面：" + link + "，开始执行===========");
		FileDownLoader fdl = new FileDownLoader();
		fdl.downloadLink(link, hfb);
		System.out.println(Thread.currentThread().getName() + "处理完毕："+ link);
		count--;
	}
}
