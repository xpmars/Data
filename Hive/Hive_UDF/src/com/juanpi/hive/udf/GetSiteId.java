package com.juanpi.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

import com.juanpi.hive.utils.URLUtils;

/**
 * 返回URL的域名，1表示juanpi，2表示9快邮，-999 表示站外
 * @author 吕鹏
 * 用法：GetSiteId(String url)
 */
public class GetSiteId extends UDF  {
	
    /**
	 * @param url, 传入的url
	 * @return 1表示juanpi，2表示9快邮，-999 表示站外
	 */
	public Integer evaluate(String url) {
		if(url==null || url.trim().length()<4){
			return null;
		}
		url = url.toLowerCase().trim();
		if (URLUtils.getHostFromURL(url) != null) {
			if ("juanpi.com".equals(URLUtils.getHostFromURL(url))) {
				return 1;
			}else if ("jiukuaiyou.com".equals(URLUtils.getHostFromURL(url))) {
				return 2;
			}else {
				return -999;
			}
		}
		
        return null;
	}		  

	public static void main(String[] argc){
		GetSiteId gh=new GetSiteId(); 
		System.out.println(gh.evaluate("http://shop.juanpi.com/deal/1201416"));		
		System.out.println(gh.evaluate("http://www.jiukuaiyou.com/deal/72210373"));
		System.out.println(gh.evaluate("http://www.baidu.com/deal/72210373"));
	}

}
