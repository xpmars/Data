package com.juanpi.hivetools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class Utils {

	public static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");	
	
	public static final String DATE_REGEX = "^[YyMmDd]{0,1}[-|+]*[0-9]*$";

	public static final String BEGIN = "{$";

	public static final String END= "}";
		
	public static String getCommandFromFile(File file) throws IOException{
		BufferedReader bf =new BufferedReader(new FileReader(file));
		StringBuilder sql=new StringBuilder();
		String temp=null;
		while((temp=bf.readLine())!=null){
			String temp1=temp.trim();
			if(temp1.length()==0 || temp1.startsWith("#") ||temp1.startsWith("--"))
				continue;
			sql.append(temp1+" ");
		}		
		return sql.toString();
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
			long time = begin.getTime()+ 1000* 3600 * 24 * offset;
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
			String suffix= res.substring(begin+BEGIN.length());
			int end=begin +BEGIN.length() + suffix.indexOf(END);			
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
	
	public static String filterInitString(String source){
		
		StringBuilder result=new StringBuilder();
		String[] sqls =source.split(";");		
		List<String> resSql=new ArrayList<String>();
		for(int i=0; i<sqls.length;i++){
			String sql=sqls[i].toLowerCase();
			if(sql.contains("add") && sql.contains("jar")){
				continue;
			}
			if(sql.contains("create") && sql.contains("temporary") && sql.contains("function")){
				continue;
			}
			if(sql.trim().length()>0){
				resSql.add(sqls[i]);
			}						
		}
		if(resSql.size()==0){
			return "";
		}		
		for(int i=0;i<resSql.size();i++){
			result.append(resSql.get(i)+"; ");
		}			
		return result.toString();
	}
	
	
	
	
	
	
	public static void main(String[] argc){
		System.out.println(parseCommand("select * from test where ds={$m-1}"));		
	}	
}
