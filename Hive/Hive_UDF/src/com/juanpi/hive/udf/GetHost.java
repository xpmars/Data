package com.juanpi.hive.udf;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import com.juanpi.hive.utils.URLUtils;

/**
 * 获取url 对应的主机， 格式为 'juanpi.com' 'google.com.cn'等，对于类似'search.juanpi.com'的只取'yihaodian.com'.
 * @author zhuokun
 * 用法：GetHost(String url)
 */
@Description(name = "GetHost", value = "_FUNC_(string1 url) - Return the host of the URL.")
public class GetHost extends UDF  {
	
	/**
	 * 获取url 对应的主机， 格式为 'juanpi.com' 'google.com.cn'等，对于类似'search.juanpi.com'的只取'yihaodian.com'.
	 * @param s url 
	 * @return 主机字符串
	 */
	public String evaluate(final String s) {
		if (s == null) { return null; }
		String res = URLUtils.getHostFromURL(s.toString());
		if(res == null){
			return null;
		}else{
			return res;
		}		
	}		  

	/**
	 * 同上，对字符串数组做批处理。
	 * @param s
	 * @return
	 */
	public List<String> evaluate(final List<String> s) {
		if (s == null) { return null; }
		List<String> result= new ArrayList<String>();
		for(String str:s){	
			if(str==null ||evaluate(str)==null ){
				result.add("");
			}else{
				result.add(evaluate(str));
			}			
		}
		return result;				
	}
	
	
	
	
	
	public static void main(String[] argc){
		GetHost gh=new GetHost(); 
		System.out.println(gh.evaluate("https://g1a183.mail.163.com./hkhpp?opet"));		
		System.out.println(gh.evaluate("https://g1a183.mail.163.com:8080/hkhpp?opet"));		
		System.out.println(gh.evaluate("http://10.1.255.3:8080/"));	
		
		System.out.println(gh.evaluate("http://www.111.com.cn/%"));
		System.out.println(gh.evaluate("http://search.111.com.cn%"));
		System.out.println(gh.evaluate("http://10.1.255.3:8080/"));		
	
		System.out.println(gh.evaluate("http:///"));

		
		
	}
}
