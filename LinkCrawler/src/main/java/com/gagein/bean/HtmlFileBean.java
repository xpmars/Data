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

package com.gagein.bean;

public class HtmlFileBean {
	//原始url地址
	private String url;
	//本地存放下载文件的目录名
	private String tempName;
	//url域名
	private String domain;
	//url下载的本地路径
	private String dirPath;
	//url下载后的文件名
	private String fileName;
	//url下载后的文件路径名
	private String mainFilePath;
	//url相关关键字
	private String keyword;
	//html文件的编码
	private String charset;
	
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	public String getDirPath() {
		return dirPath;
	}
	public void setDirPath(String dirPath) {
		this.dirPath = dirPath;
	}
	public String getFileName() {
		return fileName;
	}
	public void setFileName(String fileName) {
		this.fileName = fileName;
	}
	public String getMainFilePath() {
		return mainFilePath;
	}
	public void setMainFilePath(String mainFilePath) {
		this.mainFilePath = mainFilePath;
	}
	public String getKeyword() {
		return keyword;
	}
	public void setKeyword(String keyword) {
		this.keyword = keyword;
	}
	public String getCharset() {
		return charset;
	}
	public void setCharset(String charset) {
		this.charset = charset;
	}
	public String getTempName() {
		return tempName;
	}
	public void setTempName(String tempName) {
		this.tempName = tempName;
	}
	public String getDomain() {
		return domain;
	}
	public void setDomain(String domain) {
		this.domain = domain;
	}

}
