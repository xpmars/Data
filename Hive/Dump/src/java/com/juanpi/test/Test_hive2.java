package com.juanpi.test;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


public class Test_hive2 {

	/**
	 * @param args
	 * @throws SQLException 
	 * @throws Exception 
	 */
	public static void main(String[] args) throws SQLException, Exception {
	 
//		Flow.printCreateStatement("dw.dim_position_track",
//							"dw.dim_position_track".toLowerCase(),
//							1,
//							"5");
		

		org.apache.hive.jdbc.HiveDriver driver_new=new org.apache.hive.jdbc.HiveDriver();
		DriverManager.registerDriver(driver_new);
		Connection con1 = DriverManager.getConnection("jdbc:hive2://10.4.11.4:10000/default");
		String sqlString= "select * from archivedb.yhd_so where ds='2008-06-30' limit 2 ";
		Statement stmt = con1.createStatement();	   

		ResultSet rSet = stmt.executeQuery(sqlString);
		while (rSet.next())
		{
			System.out.println("ture");
			System.out.println(rSet.getString(1));
			System.out.println(rSet.getString(2));
			System.out.println(rSet.getString(3));
		}
		System.out.println("false");
		rSet.close();
		stmt.close();
		con1.close();
		
		
	}

		

}
