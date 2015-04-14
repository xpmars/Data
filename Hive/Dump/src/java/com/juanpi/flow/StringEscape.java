package com.juanpi.flow;

public class StringEscape {

	public static final char[] special_char= {};
	
	
	public static enum DB_STRING_COLUMN	{
		ORACLE_VARCHAR2,
		MYSQL_VARCHAR
	}

	
	/**
	 * 判断字符在数据库中的存储长度，乱码就直接返回0
	 * @param c
	 * @return
	 */
	public static int bytesInDB(char c, DB_STRING_COLUMN col){
		if(DB_STRING_COLUMN.ORACLE_VARCHAR2.equals(col)){
			if(c>=32 &&c<=126){
				return 1;
			}
			if(c>='一'&& c<='龟'){
				return 3;
			}
		}
		
		if(DB_STRING_COLUMN.MYSQL_VARCHAR.equals(col)){
			if(c>=32 &&c<=126){
				return 1;
			}
			if(c>='一'&& c<='龟'){
				return 2;
			}
		}		
		return 0;
	}
	
	/**
	 * 判断字符在数据库中的存储长度，乱码就直接返回0， 针对字符串的重载版本
	 * @param c
	 * @return
	 */
	public static int bytesInDB(String s, DB_STRING_COLUMN col){
		int len=0;		
		for(int i=0; i<s.length();i++){
			len+=bytesInDB(s.charAt(i), col);
		}
		return len;
	}
	
	/**
	 * 移除字符串中的乱码
	 * @param s
	 * @return
	 */	
	public static String validate(String s){
		if(s==null||s.length()==0){
			return null;
		}
		StringBuilder r=new StringBuilder();
		for(int i=0; i<s.length();i++){
			if(bytesInDB(s.charAt(i), DB_STRING_COLUMN.ORACLE_VARCHAR2)>0){
				r.append(s.charAt(i));
			}
		}
		return r.toString();		
	}

	/**
	 * 移除字符串中的乱码，并根据数据库指定长度进行截断
	 * @param s 原始字符串
	 * @param bytesLimit 长度限制
	 * @return
	 */
	
	public static String validate(String s, int bytesLimit, DB_STRING_COLUMN col){
		if(s==null||s.length()==0){
			return null;
		}
		int dbSizes=0;		
		StringBuilder r=new StringBuilder();
		for(int i=0; i<s.length();i++){
			if(dbSizes+ bytesInDB(s.charAt(i), col)> bytesLimit){
				break;
			}
			if(bytesInDB(s.charAt(i), col)>0){
				r.append(s.charAt(i));
				dbSizes+= bytesInDB(s.charAt(i), col);
			}
		}
		return r.toString();		
	}
	
	
	public static String validate(String s, int bytesLimit){
		return validate(s, bytesLimit, DB_STRING_COLUMN.ORACLE_VARCHAR2);
	}
	
	public static String escapingSqlString(String sql){
		return sql.replaceAll("'", "/").replaceAll(";", ".").replaceAll("\"", "/");
	}
	
	public static void main(String[] argc){
		System.out.println( 
				escapingSqlString("update rpt_flow_task set status=2, message='Error while processing statement: FAILED: ParseException " +
						"line 1:210 cannot recognize input near 'fe0' ';' 'fw' in expression specification' where id=11323"));
	}
	
	
}
