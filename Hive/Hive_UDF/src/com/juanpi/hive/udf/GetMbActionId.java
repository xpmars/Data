package com.juanpi.hive.udf;

import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDF;

import com.juanpi.hive.utils.IDChecker;
import com.juanpi.hive.utils.IDChecker.Page_Pattern;
/**
 * 获取移动App端页面类型，类型与数据库mobile.dim_page_position 中定义的ID一致
 * @author qingtian
 * 用法：GetMbActionId(String actionName)
 */
public class GetMbActionId extends UDF{
	
	public Integer evaluate(final String s) {
		IDChecker.getInstance("com/juanpi/hive/props/MbActionID.properties");
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
		GetMbActionId gh=new GetMbActionId(); 
		System.out.println(gh.evaluate("Click_home_navigation"));
	}
	
}
