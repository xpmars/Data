package com.juanpi.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import com.juanpi.hive.utils.PageTypeUtils;
/**
 * 获取页面类型，类型与数据库dim_page_type 中定义的Page_ID 一致，特殊返回值介绍：-1表示站内未知页，-999表示站外页，非法URL或空URL时返回null。
 * @author zhuokun
 * 用法：getPageid(String url)
 */
@Description(name = "GetPageID", value = "_FUNC_(string url1) - Return the page id of  url, can batch used.")
public class GetPageID extends UDF  {
	
	/**
	 * 获取指定url 对应的一级页面类型
	 * @param s
	 * @return -1表示内部未知页，-999表示外部页
	 */
	public Integer evaluate(final String s) {
		return PageTypeUtils.getPageID(s);
	}	
	
	

	public static void main(String[] argc){
		GetPageID gh=new GetPageID(); 
		String url = "http://www.juanpi.com/all?page=2" ;
		System.out.println(gh.evaluate(url));
	}
}
