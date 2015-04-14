package com.juanpi.hive.udf;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import com.juanpi.hive.utils.PageTypeUtils;
/**
 * 获取页面参数， pagevalue的值是goodid 。
 * @author 白菜
 * 用法：getPageValue(String url)
 */
@Description(name = "GetPageValue", value = "_FUNC_(string url1) - Return the page Value of  url, can batch used.")
public class GetPageValue extends UDF  {
	private static String clike= "click/?";
	private static String id= "id=";
	private static String and= "&";
	private static StringBuilder sb = new StringBuilder();
	
	/**
	 * 获取指定url 对应的一级页面类型
	 * @param s
	 * @return page value
	 */
	public String evaluate(String s) {
		String result = "";
		int index = 0;
		
		if ( s == null ) s = "";
		
		s = s.toLowerCase();
		Integer val = PageTypeUtils.getPageID(s);
		
		if ( val == null ) return result;
		
		if ( val == 28 || val == 29) {
		
			index = s.indexOf(clike);
			if ( index >= 0 ) 
				result = s.substring(index + 7).trim();
			
			index = result.indexOf(id);
			if ( index >= 0 )
				result = result.substring(index + 3).trim();
			
			index = result.indexOf(and);
			if ( index >= 0 )
				result = result.substring(0, index).trim();
				
			result = decodeGoodid(result);
			
			if ( val == 29 && result.isEmpty() ) {
				Pattern pattern = Pattern.compile("click/auth/(\\d+)");
	            Matcher matcher = pattern.matcher(s);
	            if (matcher.find()) {
	            	result = matcher.group(1);
	            }
					
				result = decodeGoodid(result);
			}
			
		}
		
		if ( val == 12 || val == 14 || val == 25 || val == 26) {
			
			Pattern pattern = Pattern.compile("deal/(\\d+)");
            Matcher matcher = pattern.matcher(s);
            if (matcher.find()) {
            	result = matcher.group(1);
            }
				
			result = decodeGoodid(result);
		}
		
		return result;
	}	
	
	/**
     * decodeGoodid:goodid解码 <br/>
     *
     * @param goodid
     * @return String
     * TODO Description <br/>
     */
    public static String decodeGoodid(String goodid) {

        try{
            if (goodid.trim().equals(StringUtils.EMPTY)) {
                return goodid;
            }
            int l = goodid.length();
            Map<Integer, String> tmpstr = new HashMap<Integer, String>();
            int flag = 1;
            int c = 0;
            for(int i=0;i<l;i++) {
                if(i != 0 && i % 2 == 0) {
                    flag = -flag;
                    if(flag == 1) {
                        c++;
                    }
                }
                if(i == l -1) {
                    for(int j=0;j<l;j++) {
                        if(tmpstr.get(j) == null) {
                            tmpstr.put(j, String.valueOf(goodid.charAt(i)));
                        }
                    }
                } else {
                    if(i % 2 == 0) {
                        if(flag == 1) {
                            tmpstr.put(((int) i/2) - c, String.valueOf(goodid.charAt(i)));
                        } else {
                            tmpstr.put(((int) l/2) + c, String.valueOf(goodid.charAt(i)));
                        }
                    } else {
                         if (flag == 1) {
                             tmpstr.put(l - (int) ((i - c * 2) / 2) - 1, String.valueOf(goodid.charAt(i)));
                         } else {
                             tmpstr.put(((int) (l / 2)) - 1 - c, String.valueOf(goodid.charAt(i)));
                         }
                    }
                }
            }
            
            TreeMap<Integer, String> treemap = new TreeMap<Integer, String>(tmpstr);
            sb.setLength(0);
            for(Entry<Integer, String> e : treemap.entrySet()) {
                sb.append(e.getValue());
            }
            goodid = String.valueOf(Long.valueOf(sb.toString()) / 7 - 201341);
        }catch(Exception e) {
            goodid = StringUtils.EMPTY;
        }
        return goodid;
    }

	public static void main(String[] argc){
		GetPageValue gh=new GetPageValue(); 
		//String url = "http://www.juanpi.com/click/?id= 680431449" ;
		String url = "http://www.juanpi.com/deal/680431449" ;
		System.out.println(gh.evaluate(url));
	}
}
