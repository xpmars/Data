package com.juanpi.hive.udf;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * 获取页面参数， pagevalue的值是goodid 。
 * @author qingtian
 * 用法：getGoodsId(String url)
 */

public class GetWapGoodsId extends UDF  {

	public String evaluate(String s) {
		String result = "";
		
		if ( s == null ) s = "";
		
		s = s.toLowerCase();
		Pattern pattern = Pattern.compile("(deal|jump|shop)/(\\d+)");
        Matcher matcher = pattern.matcher(s);
        if (matcher.find()) {
        	result = matcher.group(2);
        }
			
		result = GetGoodsId.decodeGoodid(result);
		return result;
	}
	
	

	public static void main(String[] argc){
		GetWapGoodsId gh=new GetWapGoodsId(); 
		//String url = "http://www.juanpi.com/click/?id= 680431449" ;
		String url = "http://m.juanpi.com/deal/1352416" ;
		System.out.println(gh.evaluate(url));
	}
}
