package com.juanpi.flow;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import oracle.jdbc.OracleDriver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Utils {

	private static Log log= LogFactory.getLog(Utils.class);	

	public static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");	
	
	public static final SimpleDateFormat timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");	
	
	public static final String DATE_REGEX = "^[YyMmDd]{0,1}[-|+]*[0-9]*$";

	public static final String BEGIN = "{$";

	public static final String END= "}";
		
	public static List<String> getCommandFromFile(String configFile) throws IOException{
		List<String> command = new ArrayList<String>();		
		BufferedReader bf =new BufferedReader(new FileReader(configFile));
		String sql="";
		String temp=null;
		while((temp=bf.readLine())!=null){
			if(temp.trim().startsWith("#"))
				continue;

			if(temp.trim().endsWith(";")){
				sql+=" " + temp.trim().substring(0,temp.length()-1);
				command.add(sql);
				sql="";
			}else{
				sql+= " " + temp.trim();
			}
		}
		if(sql.length()>0){
			throw new IOException("ERROR: Update SQL statement must be ended with \";\", the error statement is " + sql);
		}		
		return command;
	}

	public static void batchExecute(Connection conn, List<String> query, String prefix) throws SQLException{
		batchExecute(conn, query, prefix, null);
	}

	public static void batchExecute(Connection conn, List<String> query, String prefix, Map<String, String> param) throws SQLException{
		Statement stmt = conn.createStatement();
		if(query.size()>0){
			for(String sql:query){
				sql = parseCommand(sql, param);
				stmt.addBatch(sql);	 
				log.info(prefix +sql);
			}
			int[] status=stmt.executeBatch();

			for(int i=0; i<status.length; i++){
				if(status[i]==Statement.EXECUTE_FAILED)
					log.error("Failed to execute " + prefix + query.get(i));
			}
		}
		stmt.close();		
	}

	public static Connection getOracleConnection(String url, String user, String password) throws SQLException {		 
	//	OracleDriver driver=null;		
		Connection con = DriverManager.getConnection(url, user, password);   
		return con;
	}	 

	public static Connection getOracleConnection(String db) throws SQLException {
		Connection conn=null;
		if(db.equalsIgnoreCase("52")){
			conn = getOracleConnection("jdbc:oracle:thin:@10.0.0.52:1522/edw1", 
					"edw1_user", "yp71mhz8vq2ng");
		}else if(db.equalsIgnoreCase("55")){
			conn = getOracleConnection("jdbc:oracle:thin:@10.0.0.55:1522/track1", 
					"track1_user", "yihaodian");
		}else if(db.equalsIgnoreCase("6")){
			conn = getOracleConnection("jdbc:mysql://10.0.0.57:3306/search_b2c", 
					"searchuser", "ossd!qwe$fff");
			
		}else if(db.equalsIgnoreCase("5")){
			conn = getOracleConnection("jdbc:oracle:thin:@10.0.1.222:1521/edwstd01", 
					"edw1_user", "yp71mhz8vq2ng");
		}
		else if(db.equalsIgnoreCase("3")){
			conn = getOracleConnection("jdbc:oracle:thin:@10.0.0.8:1522/ORCL", 
					"etlqry", "ywvnhdges2hg3");
		}
		else if(db.equalsIgnoreCase("4")){
			conn = getOracleConnection("jdbc:oracle:thin:@10.0.0.15:1522/item", 
					"etlqry", "ywvnhdges2hg3");
		}
		return conn;
	}
	public static String getParsedDate(String exp){
		Date begin = new Date();
		String today = dateFormat.format(begin);
		String[] date= today.split("-");
		int year = Integer.parseInt(date[0]);
		int month = Integer.parseInt(date[1]);
		int day = Integer.parseInt(date[2]);
		int[] dateArray = new int[]{year, month, day};		

		String value = exp;
		int index = 2;
		if(exp.startsWith("y")|| exp.startsWith("Y")) {
			index=0; 
			value = value.substring(1, value.length()).trim();
		}else if(exp.startsWith("M")||exp.startsWith("m")){
			index=1;
			value = value.substring(1, value.length()).trim();
		}else if(exp.startsWith("D")||exp.startsWith("d")){
			index=2;
			value = value.substring(1, value.length()).trim();
		}

		int offset=0;
		if(value.length()>0){
			if(value.startsWith("+")){
				value=value.substring(1, value.length()).trim();
			}
			offset = Integer.valueOf(value);
		}			
		if(index<2){
			dateArray[index]= dateArray[index]+ offset;
			if(index==1){
				if(dateArray[1]<=0 || dateArray[1]>12){
					int numY=dateArray[1]/12;
					int numM = dateArray[1]%12;
					dateArray[0]= dateArray[0] + numY;
					dateArray[1]=numM;						
					if(dateArray[1]<=0){
						dateArray[0]= dateArray[0] - 1;
						dateArray[1]= dateArray[1]+12;
					}						
				}
			}				
		}else{
			
			//注意: 此处offset 必须进行往 Long 型的转化，否则会出现隐藏错误
		    //long time = begin.getTime()+ 1000* 3600 * 24 * offset;
			long time = begin.getTime()+ 1000* 3600 * 24 * (long)offset;
			return dateFormat.format(new Date(time));
		}

		if(index==0){
			return new Integer(dateArray[0]).toString();
		}
		if(index==1 && dateArray[1]<10){
			return dateArray[0]+ "-0"+ dateArray[1];
		}
		return dateArray[0]+ "-"+ dateArray[1];			
	}



	public static String parseCommand(String raw){
		return parseCommand(raw, null);
	}
	
	public static String parseCommand(String raw, Map<String, String> map){
		String res=raw;
		int begin=res.indexOf(BEGIN);
		while(begin!=-1){
			int end=res.indexOf(END);
			String tmp = res.substring(begin+BEGIN.length(), end).trim();
			if(tmp.matches(DATE_REGEX)){
				res = res.substring(0, begin) + getParsedDate(tmp) + res.substring(end+1, res.length());
			}else{
				if(map!=null && map.get(tmp)!=null){
					res = res.substring(0, begin) + map.get(tmp) + res.substring(end+1, res.length());	
				}else{
					throw new RuntimeException("Invalid expression :" + "{$" + tmp + "}.");				
				}				
			}			
			begin=res.indexOf(BEGIN);
		}		
		return res;
	}
	
	public static String decodePassword(String input){		
		String[] arr=input.split("-");
		StringBuilder builder= new StringBuilder();
		for(String str:arr){
			builder.append(str);
		}		
		return builder.toString();
	}
	

	
	public static String encode(String input, int flag){
		StringBuilder builder =new  StringBuilder();
		for(int i=0;i<input.length();i++){
			int k =(int)input.charAt(i);
			//int code = new Integer(k^flag);
			builder.append("u" + new Integer(k^flag).toString());
		}
		return builder.substring(1);
	}
	
	public static String decode(String input, int flag){
		String[] arr=input.split("u");
		StringBuilder builder= new StringBuilder();
		for(String str:arr){
			int t=Integer.valueOf(str);
			t = t ^ flag;
			builder.append((char)t);
		}		
		return builder.toString();
	}	
	
	public static String getLocalIP(){
		try {
			return InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e) {
			return "unknown";
		}		
	}
	
	
	
	

	public static void main(String[] argc){
		
		System.out.println(Double.MAX_VALUE);
		System.out.println(Long.MAX_VALUE);
		
		System.out.println("124".substring(0,3));
	/*	System.out.println(parseCommand("select * from test where ds={$m-1}"));		
		int flag=1233445;
		String test="abdfr@tee!";
		String ecode=encode(test,flag);
		System.out.println(ecode);
		System.out.println("Test is "+test + ", result is "+decode(ecode, flag));
		*/

	//	System.out.println((int)'龟');
		String test="����tete";
		//test="\\xF0\\x9F\\x8D\\x86";
	/*	for(int i=0; i<test.length(); i++){
			System.out.println((int)test.charAt(i));
			System.out.println(test.codePointAt(i));
		}
		*/
	//	System.out.println(test);
	//	System.out.println(test.replace("\\x", ""));
		System.out.println(StringEscape.validate(test));
		//System.out.println(test.replace('\x', ''));
/*		String pattern="^[\\cx]+$";
		System.out.println("t fe,+efe/*2e2".matches(pattern));
		System.out.println("����".matches(pattern));
	//	System.out.println("����".length());
		System.out.println("汉字".matches(pattern));
	*/	
	
		
		
	}
	
	
}
