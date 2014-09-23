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

package com.gagein.crawler;

import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;
import org.htmlparser.Node;
import org.htmlparser.Parser;
import org.htmlparser.filters.NodeClassFilter;
import org.htmlparser.tags.LinkTag;
import org.htmlparser.util.NodeList;
import org.htmlparser.util.ParserException;
import org.htmlparser.util.SimpleNodeIterator;

import com.gagein.bean.HtmlFileBean;
import com.gagein.filter.LinkFilter;

public class HtmlParserTool {
	/** logger */
	private static final Logger logger = Logger.getLogger(HtmlParserTool.class);

	// 获取一个网站上的链接,filter 用来过滤链接
	public static Set<String> extracLinks(HtmlFileBean hfb) {
		LinkFilter linkFilter = new LinkFilter();
		Set<String> links = new HashSet<String>();
		linkFilter.setKeyword(hfb.getKeyword());
		linkFilter.setDomain(hfb.getDomain());// 将不包含主站前缀的链接过滤掉
		try {
			Parser parser = new Parser(hfb.getMainFilePath());
			parser.setEncoding(hfb.getCharset());

			// 设置过滤器
			// OrFilter 来设置过滤 <a> 标签，和 <frame> 标签
			NodeClassFilter nodeFilter = new NodeClassFilter(LinkTag.class);
			// 得到所有经过过滤的标签
			NodeList list = parser.extractAllNodesThatMatch(nodeFilter);

			for (int i = 0; i < list.size(); i++) {
				Node tag = list.elementAt(i);
				// <a> 标签
				if (tag instanceof LinkTag) {
					LinkTag link = (LinkTag) tag;
					String linkUrl = link.getLink();// url

					if (linkUrl.startsWith("file")) {
						linkUrl = linkUrl.replace("file://localhost/", "");
						String tempName = hfb.getTempName();
						if (linkUrl.contains("/"+tempName+"/")) {
							int index = linkUrl.indexOf(tempName);
							linkUrl = linkUrl.substring(index + tempName.length());
						}
					}
					linkFilter.setText(link.getLinkText());// 链接文字
					if (linkFilter.accept(linkUrl)) {
						if (!linkUrl.startsWith("http")) {
							linkUrl = hfb.getUrl() + linkUrl;
						}
						logger.debug("========过滤后的链接：" + linkUrl);
						links.add(linkUrl);
					}
				}
			}
		} catch (ParserException e) {
			e.printStackTrace();
		}
		return links;
	}

	@SuppressWarnings("unused")
	private static void processNodeList(NodeList list, String keyword) {
		// 迭代开始
		SimpleNodeIterator iterator = list.elements();
		while (iterator.hasMoreNodes()) {
			Node node = iterator.nextNode();
			// 得到该节点的子节点列表
			NodeList childList = node.getChildren();
			// 孩子节点为空，说明是值节点
			if (null == childList) {
				// 得到值节点的值
				String result = node.toPlainTextString();
				// 若包含关键字，则简单打印出来文本
				if (result.indexOf(keyword) != -1)
					System.out.println(result);
			} // end if
				// 孩子节点不为空，继续迭代该孩子节点
			else {
				processNodeList(childList, keyword);
			}// end else
		}// end wile
	}

}
