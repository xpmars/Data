package com.juanpi.flow;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

public class Monitor {
	public static String querySql = "/opt/project/mail_monitor/setsql.sql";
	public static String countSql = "/opt/project/mail_monitor/count_alert.sql";
	public String result          = "/opt/project/mail_monitor/result.txt";
	public String resultLog          = "/opt/project/mail_monitor/result.log";
	
	public String count_result          = "/opt/project/mail_monitor/count_alert.txt";
	public String count_resultLog          = "/opt/project/mail_monitor/count_alert.log";
	/**
	 * @param args
	 */
	
	public void monitor(String fileName) throws Exception{
		File file = new File(fileName);
		FileReader reader = new FileReader(file);
		BufferedReader br = new BufferedReader(reader);
		StringBuffer sBuffer = new StringBuffer();
		String aline = null;
		while((aline=br.readLine())!=null)
		{
			if(!"".equals(aline) && !aline.startsWith("#") && !aline.startsWith("--"))
			{
				sBuffer.append(aline+" \n");
			}
		}
		
		Connection con = DBConnection.getInstance(null);
		Statement st   = con.createStatement() ;
		ResultSet rs = st.executeQuery(sBuffer.toString()) ;
		String data = "" ;
		String data2 = "TABLE_NAME\t\tAVG_CREATE_TIME\n" ;
		data2+="---------------------------------------------------------\n" ;
		while(rs.next())
		{
			data += "Hive%20monitor:"+rs.getString(1)+"%20is%20delay,%20avg%20create_time%20is%20"+rs.getString(2)+"%20in%20last%207%20days\n";
			data2 += rs.getString(1)+"\t\t"+rs.getString(2)+"\n";
		}
		File resultFile = new File(result);
		if(! resultFile.exists())
		{
			resultFile.createNewFile();
		}
		byte[] b = data.getBytes();
		FileOutputStream fs = new FileOutputStream(resultFile) ;
		fs.write(b);
		
		File log = new File(resultLog);
		if(! log.exists())
		{
			log.createNewFile();
		}
		byte[] b2 = data2.getBytes();
		FileOutputStream fs2 = new FileOutputStream(log) ;
		fs2.write(b2);
		fs2.close();
		
		fs.close();
		br.close();
		reader.close();
		rs.close();
		st.close();
	}
	public void count_monitor(String fileName) throws Exception{
		File file = new File(fileName);
		FileReader reader = new FileReader(file);
		BufferedReader br = new BufferedReader(reader);
		StringBuffer sBuffer = new StringBuffer();
		String aline = null;
		while((aline=br.readLine())!=null)
		{
			if(!"".equals(aline) && !aline.startsWith("#") && !aline.startsWith("--"))
			{
				sBuffer.append(aline+" \n");
			}
		}
		
		Connection con = DBConnection.getInstance(null);
		Statement st   = con.createStatement() ;
		ResultSet rs = st.executeQuery(sBuffer.toString()) ;
		String msg_data = "" ;
		String mail_data2 = "TABLE_NAME\t\tAVG_ROWS\t\tToday\t\tReduce\n" ;
		mail_data2+="---------------------------------------------------------\n" ;
		int alert_num = 0;
		while(rs.next())
		{
			alert_num = alert_num + 1 ;
			msg_data += "Hive%20monitor:"+rs.getString(1)+"%20rows%20is%20not%20enough,%20Reduce%20rate%20is%20"+rs.getDouble(4)+"\n";
			mail_data2 += rs.getString(1)+"\t\t"+rs.getInt(2)+"\t\t"+rs.getInt(3)+"\t\t"+rs.getDouble(4)+"\n";
		}
		File resultFile = new File(count_result);
		if(! resultFile.exists())
		{
			resultFile.createNewFile();
		}
		byte[] b = msg_data.getBytes();
		FileOutputStream fs = new FileOutputStream(resultFile) ;
		fs.write(b);
		
		File log = new File(count_resultLog);
		if(! log.exists())
		{
			log.createNewFile();
		}
		byte[] b2 = mail_data2.getBytes();
		FileOutputStream fs2 = new FileOutputStream(log) ;
		fs2.write(b2);
		fs2.close();
		
		fs.close();
		br.close();
		reader.close();
		rs.close();
		st.close();
		con.close();
	}
	
	public static void main(String[] args) throws Exception {
		Monitor monitor = new Monitor();
		if (args.length > 0) {
			querySql = args[0] ;
		}
		if (args.length > 1) {
			countSql = args[1] ;
		}
		monitor.monitor(querySql);
		monitor.count_monitor(countSql);
	}

}
