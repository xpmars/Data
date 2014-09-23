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
//          Jul 10, 2014     Administrator      Created
//
// CVS version control block - do not edit manually
// $Id: $

package com.gagein.filter;

import org.htmlparser.Node;
import org.htmlparser.NodeFilter;

public class FrameFilter implements NodeFilter {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public boolean accept(Node node) {
		if (node.getText().startsWith("frame src=")) {
			return true;
		} else {
			return false;
		}

	}

}
