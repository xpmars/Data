package com.juanpi.tools;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.juanpi.flow.DBConnection;

public class Tools {

	
	public static void reportExport(String tableName, String taskName,String dump_type,String db_ip, int row_count, String begin_time, String end_time)
	throws Exception 
	{
		Connection conn=DBConnection.getInstance();
		Statement stat=conn.createStatement();
		StringBuffer sBuffer = new StringBuffer();
		sBuffer.append("insert into rpt_batch_hive_log(table_name,job_name,DUMP_TYPE,source_db,dest_db,row_count,flag,begin_time,end_time)") ;
		sBuffer.append("values (") ;
		sBuffer.append(" '" + tableName + "',");
		sBuffer.append(" '" + taskName + "',");
		sBuffer.append(" '" + dump_type + "',");
		sBuffer.append(" '" + getLocalHostIP() +"',");
		sBuffer.append(" '" + db_ip +"',");
		sBuffer.append( row_count + ",0,");
		//oracle
//		sBuffer.append(" to_date('"+begin_time+"','yyyy-mm-dd hh24:mi:ss'),");
//		sBuffer.append(" to_date('"+end_time+"','yyyy-mm-dd hh24:mi:ss')");
		
		//mysql str_to_date('2010-11-22 14:39:51','%Y-%m-%d %H:%i:%s') 
		sBuffer.append(" str_to_date('"+begin_time+"','%Y-%m-%d %H:%i:%s'),");
		sBuffer.append(" str_to_date('"+end_time+"','%Y-%m-%d %H:%i:%s')");
		sBuffer.append(" )") ;
		
		stat.execute(sBuffer.toString());
//		conn.commit();
		stat.close() ;
	}
	public static String getLocalHostIP()
	{
		InetAddress address;
		try {
			address = InetAddress.getLocalHost();
			return address.getHostAddress();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		return null;
		
	}
	
	public static String getNow()
	{
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return sdf.format(new Date());
	}
	public static void main(String[] args) {
//		System.out.println(getNow());
		System.out.println("123".split("\\.").length);
	}
	
}
