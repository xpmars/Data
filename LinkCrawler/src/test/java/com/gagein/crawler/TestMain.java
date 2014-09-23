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
//          Jul 9, 2014     Administrator      Created
//
// CVS version control block - do not edit manually
// $Id: $

package com.gagein.crawler;

import java.io.File;
import java.io.IOException;


import com.gagein.dp.platform.common.api.util.HttpUtil;
import com.gagein.dp.platform.common.api.util.HttpUtil.HttpUtilResponse;

import org.junit.Test;

public class TestMain {
	private String dirPath;

	@Test
	public void TestDownLoadHtml() throws IOException{
		String website = "http://www.amazon.cn";
		setDirPath(System.getProperty("user.dir") + File.separator + "temp" + File.separator + 
				website.substring(7).split("\\.")[1]);
		try {
            HttpUtilResponse res = HttpUtil
                    .retrievePageInfo("http://www.amazon.cn");
            if (res != null) {
                System.out.println(res.getStatusLine());
                System.out.println(res.getFinalurl());
            } else {
                System.out.println("on");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
	}


	public String getDirPath() {
		return dirPath;
	}

	public void setDirPath(String dirPath) {
		this.dirPath = dirPath;
	}
}
