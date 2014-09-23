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

package com.gagein.structure;

import java.util.HashSet;
import java.util.Set;

public class LinkDB {
	// 已访问的 url 集合
	private static Set<String> visitedUrl = new HashSet<String>();

	// 待访问的 url 集合
	private Queue<String> unVisitedUrl = new Queue<String>();

	public Set<String> getVisitedUrl() {
		return visitedUrl;
	}

	public void setVisitedUrl(Set<String> visitedUrl) {
		LinkDB.visitedUrl = visitedUrl;
	}
	
	public Queue<String> getUnVisitedUrl() {
		return unVisitedUrl;
	}

	public void addVisitedUrl(String url) {
		visitedUrl.add(url);
	}

	public void removeVisitedUrl(String url) {
		visitedUrl.remove(url);
	}

	public String unVisitedUrlDeQueue() {
		return unVisitedUrl.deQueue();
	}

	// 保证每个 url 只被访问一次
	public void addUnvisitedUrl(String url) {
		if (url != null && !url.trim().equals("") && !visitedUrl.contains(url)
				&& !unVisitedUrl.contians(url))
			unVisitedUrl.enQueue(url);
	}

	public int getVisitedUrlNum() {
		return visitedUrl.size();
	}

	public boolean unVisitedUrlsEmpty() {
		return unVisitedUrl.empty();
	}
}
