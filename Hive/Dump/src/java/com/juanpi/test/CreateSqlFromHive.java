package com.juanpi.test;


import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.hive.jdbc.HiveDriver;


public class CreateSqlFromHive {

	/**
	 * @param args
	 * @throws SQLException 
	 * @throws Exception 
	 */
	public static void main(String[] args) throws SQLException, Exception {
	 
		InputStream stream =ClassLoader.getSystemClassLoader().getResourceAsStream("com/yihaodian/test/table.list");
		BufferedReader reader = new BufferedReader(new InputStreamReader(stream, "UTF-8"));			
		Statement stmt  = null;
		ResultSet rSet  = null;
		String location = null;	
		String tmp = null;
		String[] arr = null;
		
		HiveDriver driver=new HiveDriver();
		DriverManager.registerDriver(driver);
		Connection con1 = DriverManager.getConnection("jdbc:hive2://10.4.11.4:12000/default");
		stmt = con1.createStatement();	   
		
		while((tmp=reader.readLine())!=null && !tmp.startsWith("#"))
		{
			String tableName = null;	
			String ds = null;	
			arr = tmp.split("	") ;
			if (arr.length >= 1) {
				tableName = arr[0] ;
			}
			if(arr.length >= 2)
			{
				ds = arr[1] ;
			}
			if(arr.length >= 3)
			{
				location = arr[2] ;
			}
			String sqlString= "select * from "+tableName+" where 1=0 ";
			
			rSet = stmt.executeQuery(sqlString);
			ResultSetMetaData meta2 = rSet.getMetaData() ;
//			while (rSet.next())
//			{
//				System.out.println("ture");
//				System.out.println(rSet.getString(1));
//			}
//			System.out.println("false");
			String create = "create table "+tableName+" (" ;
			for(int i=1;i<=meta2.getColumnCount();i++){
				create += meta2.getColumnName(i)+" "+meta2.getColumnTypeName(i)+",";
			}
			create = create.substring(0, create.length() - 1) ;
			create += " ) " ;
			if (ds != null && !"".equals(ds)) {
				create += "partitioned by("+ds+"  string)" ;
			}
			create += " ;";
			System.out.println(create);
	    }
		rSet.close();
		stmt.close();
		con1.close();
		
	}

		

}
