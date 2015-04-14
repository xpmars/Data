package com.juanpi.tools;

import java.io.File;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.log4j.Logger;

import com.juanpi.flow.DBConnection;

public class HiveLogUtils {

	/**
	 * @param args
	 */
	public static Logger log = Logger.getLogger(HiveLogUtils.class);
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		if (args.length == 2) {
			getHiveLog(args[0],args[1]) ;
		}else if (args.length >= 3) {
			insertHiveLog(args);
		}
		
	}
	public static void insertHiveLog(String[] args){
		log.info("insert 1rows into rpt_batch_hive_log ! ") ;
		String tableName = args[0].trim() ;
		String begintime = args[1].trim() ;
		String endtime = args[2].trim() ;
		int count = 0 ;
		if (args.length>=4 && args[3] != null && !"".equals(args[3])) {
			count = Integer.parseInt(args[3].trim()) ;
		}
		try {
//			Tools.reportExport(tableName, tableName, null, count, begintime, endtime) ;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static String getHiveLog(String tableName,String dateString)
	{
		String fileName = "/opt/workspace/.trackinfo";
		if (tableName == null || "".equals(tableName) ||dateString == null || "".equals(dateString)) {
			log.info("args is not enough  ! ") ;
			return null;
		}
		String result = null  ;
		try {
			Connection con = DBConnection.getInstance();
			Statement st = con.createStatement() ;
			
			if (con == null) {
				con = DBConnection.getInstance();
			}
			if(st == null){
				st = con.createStatement();
			}
			String sql = "select to_char(create_time,'YYYY-MM-DD') from rpt_batch_hive_log where create_time >=date'"+dateString.trim()+"' and lower(table_name)='"+tableName.toLowerCase()+"'" ;
			log.info(sql) ;
			ResultSet rs = st.executeQuery(sql) ;
			while (rs.next()) {
				result = rs.getString(1);
			}
			File file = new File(fileName) ;
			if(! file.exists())
			{
				file.createNewFile();
			}
			byte[] b = result.getBytes();
			FileOutputStream fs = new FileOutputStream(file);
			fs.write(b);
			fs.close();
			
			
			rs.close() ;
			st.close() ;
			con.close() ;
		} catch (Exception e) {
			e.printStackTrace() ;
		}
		return result;
	}
}
