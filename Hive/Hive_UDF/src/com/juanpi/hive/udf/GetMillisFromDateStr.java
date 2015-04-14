package com.juanpi.hive.udf;

import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * 根据传人的日期字符串如2014-11-20 ，返回毫秒字符串如1416541278395
 * 用法：getMills(String dateStr)
 */
public class GetMillisFromDateStr extends UDF  {

	public SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	public String evaluate(String dateStr) {
		if(dateStr == null)
		{
			return null;
		}
		try
		{
			Calendar c = Calendar.getInstance() ;
			
			c.setTime(sdf.parse(dateStr)) ;
			return c.getTimeInMillis() + "" ;
		}
		catch(Exception e){
			return null;
		}
	}
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		GetMillisFromDateStr g = new GetMillisFromDateStr();
		System.out.println(g.evaluate("2014-11-21")) ;
		System.out.println(System.currentTimeMillis()) ;
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println(System.currentTimeMillis()) ;
	}

}
