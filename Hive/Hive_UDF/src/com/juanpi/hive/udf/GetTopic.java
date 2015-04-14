package com.juanpi.hive.udf;


import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.ql.exec.UDF;
/**
 * 获取专题name
 * @author qingtian
 * 用法：getTopic(URL)
 */
public class GetTopic extends UDF{
	
	public String evaluate(final String url) {
		if(url == null || url.trim().length()==0)
		{
			return null;
		}
		Pattern p = Pattern.compile("zhuanti/([a-zA-Z0-9]+)");
        Matcher m = p.matcher(url);
        if (m.find()) {
        	return m.group(1).toLowerCase();
        }
		return null;
	}
	
	
	public static void main(String[] argc){
		GetTopic gh=new GetTopic(); 
		System.out.println(gh.evaluate("http://www.juanpi.com/zhuanti/qingREnj3ie?utm"));
	}
	
}
