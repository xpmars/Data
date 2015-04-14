package com.juanpi.hive.udf;

import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDF;

import com.juanpi.hive.utils.IDChecker;
import com.juanpi.hive.utils.IDChecker.Page_Pattern;

/**
 * 获取移动端页面类型，类型与数据库wap_page_type 中定义的ID一致
 * @author qingtian
 * 用法：GetWapPageId(String url)
 */
public class GetWapPageId extends UDF{
	
	public Integer evaluate(final String s) {
		if(s==null || s.trim().length()==0){
			return null;
		}
		IDChecker.getInstance("com/juanpi/hive/props/WapPageID.properties");
		List<Page_Pattern> patterns = IDChecker.patterns;
		if(patterns==null || patterns.isEmpty()) return -1;
		for (Page_Pattern pat : patterns) {
			if (pat.match(s.toLowerCase())) {
				return pat.id;
			}
		}
		return -1;
	}
	
	
	public static void main(String[] argc){
		GetWapPageId gh=new GetWapPageId(); 
		System.out.println(gh.evaluate("http://m.juanpi.com/all/meishi/2"));
	}
	
}
