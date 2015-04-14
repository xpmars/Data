package com.juanpi.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
/**
 * 用户去关键页list
 *   -1为取关键页list最后一个，-2是倒数第二个，0为第一个，1为第2个
 * @author zhuokun
 *
 */
public class GetSplitIndex extends UDF{
	
	public String evaluate(String value, String splitBy, Integer index) {
		String[] arr = null;
		try {
			arr = value.split(splitBy);
			if (arr.length <= 0) {
				return null;
			}
			if (index >= 0) {
				if (arr.length >= Math.abs(index) + 1) {
					return arr[index];
				} else {
					return null;
				}
			}
			// 负数时
			if (index < 0) {
				if (arr.length >= Math.abs(index)) {
					// arr.length - 1  最大
					int inx = arr.length - Math.abs(index) ;
					return arr[inx];
				} else {
					return null;
				}
			}
		} catch (Exception e) {
			return null;
		}

		return null;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		System.out.println(new GetSplitIndex().evaluate("123,345,12,4", ",", 0)  );
		System.out.println(new GetSplitIndex().evaluate("123,345,12,4", ",", -1)  );
		System.out.println(new GetSplitIndex().evaluate("123,345,12,4", ",", -5)  );
	}

}
