package com.juanpi.hive.udf;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * 根据传入毫秒字符串如1416541278395, 返回日期字符串如2014-11-20 
 * 用法：GetDateFromMills(String timeStamp) 或者 GetDateFromMills(Long timeStamp)
 */
public class GetDateFromMills extends UDF  {

	public SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	public String evaluate(String mills) {
		if(mills == null)
		{
			return null;
		}
		
		try
		{
			long m = Long.parseLong(mills);
			Calendar c = Calendar.getInstance() ;
			
			c.setTime(new Date(m)) ;
			return sdf.format(c.getTime());
		}
		catch(Exception e){
			return null;
		}
	}
	public String evaluate(Long mills) {
		if(mills == null)
		{
			return null;
		}
		
		try
		{
			Calendar c = Calendar.getInstance() ;
			
			c.setTime(new Date(mills)) ;
			return sdf.format(c.getTime());
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

		GetDateFromMills g = new GetDateFromMills();
		System.out.println(g.evaluate("1414659039684")) ;
		Long aLong=1414659039684l;
		System.out.println(g.evaluate(aLong)) ;
//		try {
//			Thread.sleep(2000);
//		} catch (InterruptedException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		System.out.println(System.currentTimeMillis()) ;
	}

}
