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

import java.util.LinkedList;
/**
 * 数据结构队列
 */
public class Queue<T> {

	private LinkedList<T> queue=new LinkedList<T>();
	
	public void enQueue(T t)
	{
		queue.addLast(t);
	}
	
	public T deQueue()
	{
		return queue.removeFirst();
	}
	
	public boolean isQueueEmpty()
	{
		return queue.isEmpty();
	}
	
	public boolean contians(T t)
	{
		return queue.contains(t);
	}
	
	public boolean empty()
	{
		return queue.isEmpty();
	}
	
	public int getSize()
	{
		return queue.size();
	}
}