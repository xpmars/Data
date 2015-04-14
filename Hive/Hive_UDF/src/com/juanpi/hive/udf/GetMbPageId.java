package com.juanpi.hive.udf;

import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDF;

import com.juanpi.hive.utils.IDChecker;
import com.juanpi.hive.utils.IDChecker.Page_Pattern;

/**
 * 获取移动端页面类型，类型与数据库dim_page 中定义的ID一致
 * @author qingtian
 * 用法：GetMbPageId(String pageName)
 */
public class GetMbPageId extends UDF{
	
	public Integer evaluate(final String s) {
		//List<Page_Pattern> patterns = new MbPageIDChecker("com/juanpi/hive/props/MbPageID.properties").patterns;
		//IDChecker mc = new IDChecker("com/juanpi/hive/props/MbPageID.properties");
		IDChecker.getInstance("com/juanpi/hive/props/MbPageID.properties");
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
		GetMbPageId gh=new GetMbPageId(); 
		System.out.println(gh.evaluate("Page_mother"));
	}
	
}