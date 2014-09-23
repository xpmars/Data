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

package com.gagein.filter;

public class LinkFilter implements IFilter {
	/** startUrl for Discovery */
	private String domain;
	/** keyword for Discovery */
	private String keyword;
	/** text for Discovery */
	private String text;

	public boolean accept(String linkUrl) {
		if (linkUrl.contains(domain)){
			return true;
		}
		else
			return false;
	}

	public String getKeyword() {
		return keyword;
	}

	public void setKeyword(String keyword) {
		this.keyword = keyword;
	}

	public String getText() {
		return text;
	}

	public void setText(String text) {
		this.text = text;
	}

	public String getDomain() {
		return domain;
	}

	public void setDomain(String domain) {
		this.domain = domain;
	}

}
