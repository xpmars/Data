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

public interface IFilter {
	public boolean accept(String url);
}