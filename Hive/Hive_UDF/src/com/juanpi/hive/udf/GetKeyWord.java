package com.juanpi.hive.udf;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.ql.exec.UDF;

import com.juanpi.hive.utils.DecodeURLParam;

/**
 * 从关键词搜索页URL中返回关键词，获取不到返回null。用法示例: getKeyWord(url)
 * @author zhuokun
 * 用法：getKeyword(String url)
 */
public class GetKeyWord extends UDF  {
	
    /**
	 * 关键词搜索页URL中的到关键词， 返回关键词
	 * @param url, 关键词搜索页面url
	 * @return 若URL中有关键词则返回转换为中文的关键词，无则返回0
	 */
	public String evaluate(final String url) {
		try {
		    String result = null;
		    String res = null;
		    if (url == null) { 
		        return result; 
		    }
		    DecodeURLParam d = new DecodeURLParam();
	        //String res = URLUtils.getUrlParam(url.toString(), param.toString());
	        String urlString = url.trim().replace("_", "--").toLowerCase();
	        //res = StringUtils.multipleSplit(urlString, "k~1", "[/]","[&]","[-]","[?]");
	        Pattern pattern = Pattern.compile("keywords=([\u4E00-\u9FA5\\s0-9A-Za-z%+-]+)");
			Matcher matcher = pattern.matcher(urlString);
			Pattern pattern1 = Pattern.compile("keyword=([\u4E00-\u9FA5\\s0-9A-Za-z%+-]+)");
			Matcher matcher1 = pattern1.matcher(urlString);
//			Pattern pattern2 = Pattern.compile("searchproduct--([0-9]+)--([\u4E00-\u9FA5\\s0-9A-Za-z%+]+)([0-9-]?)");
//			Matcher matcher2 = pattern2.matcher(urlString);
//			Pattern pattern3 = Pattern.compile("adword/+([\u4E00-\u9FA5\\s0-9A-Za-z%+]+)");
//			Matcher matcher3 = pattern3.matcher(urlString);
//			Pattern pattern4 = Pattern.compile("searchproductlist--([0-9]+)--([\u4E00-\u9FA5\\s0-9A-Za-z－%+]+)([0-9-]?)");
//			Matcher matcher4 = pattern4.matcher(urlString);
//			Pattern pattern5 = Pattern.compile("pad/searchpro/([0-9]+/[0-9]+/)([\u4E00-\u9FA5\\s0-9A-Za-z%+]+)");
//			Matcher matcher5 = pattern5.matcher(urlString);
//			Pattern pattern6 = Pattern.compile("([/-]+)k=([\u4E00-\u9FA5\\s0-9A-Za-z%+-]+)");
//			Matcher matcher6 = pattern6.matcher(urlString);
			if(matcher.find()){
				res = matcher.group(1).replace("--", "_");
				result = d.evaluate(res);
			}else if(matcher1.find()){
				res = matcher1.group(1).replace("--", "_");
				result = d.evaluate(res);
			}
//			}else if(matcher2.find()){
//				res = matcher2.group(2).replace("--", "_");
//				result = res;
//			}else if(matcher3.find()){
//				res = matcher3.group(1).replace("--", "_");
//				result = res;
//			}else if(matcher4.find()){
//				res = matcher4.group(2).replace("--", "_");
//				result = res;
//			}else if(matcher5.find()){
//				res = matcher5.group(2).replace("--", "_");
//				result = res;
//			}else if(matcher6.find()){
//				res = matcher6.group(2).replace("--", "_");
//				result = res;
//			}
			//无法正常转码的返回null
	        if (result.indexOf("\\") != -1 || result.indexOf("%") != -1) {
				return null;
			}
			
	        return result;
		} catch (Exception e) {
			return null;
		}
	}		  

	public static void main(String[] argc){
		GetKeyWord gh=new GetKeyWord(); 
//		System.out.println(gh.evaluate("http://m.yhd.com/searchproductlist_5_减肥 瘦身_77_5"));
//		System.out.println(gh.evaluate("http://m.yhd.com/searchproductlist_5_减肥 瘦身_99_2"));
//		System.out.println(gh.evaluate("http://m.yhd.com/mw/adword/减肥 瘦身"));
		System.out.println(gh.evaluate("http://m.juanpi.com/mw/search?keywords=%E6%B4%97%E8%A1%A3%E6%B6%B2&searchid=1&serchtype="));
		System.out.println(gh.evaluate("http://www.juanpi.com/search?keywords=%E5%9B%B4%E5%B7%BE"));


	}

}
