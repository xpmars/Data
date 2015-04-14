package com.juanpi.hive.udf;

import org.apache.commons.lang.NumberUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * 返回decoding code
 * @author baicai
 * 用法：decoding(String)
 */
public class Decoding extends UDF {
	
	/**
	 * @param String value,  encoding code
	 * @return String,   decoding code
	 */
	public static Long evaluate(String value) {
        Long result = null;
		/*if ( value.indexOf(";") >= 0) {
	        value = value.substring(0, value.indexOf(";"));
	    }*/
		if ( value == null ) {
			return null;
		}
	    
	    /*if ( value.trim().equals(StringUtils.EMPTY) ) {
	        return 0;
	    }*/
	    
	    try{
	        value = String.valueOf(Long.parseLong(value, 36) - 60512868);
	    }catch(Exception e) {
	        value = StringUtils.EMPTY;
	    }
	    
	    if ( NumberUtils.isDigits(value) ) {
	    	try{
		    	result = Long.valueOf(value);
		    }catch(Exception e) {
		    	result = Long.valueOf(0);
		    }
	    	return result;
	    }else{
	    	return Long.valueOf(0);
	    }

		//return value;

    }
	
	public static void main(String[] args) {
		//12lo3j
		//110jhe
		//10fbyy
		String ls = "1409234956688";
		Long val = evaluate(ls);
		System.out.println(val);
	}

}
