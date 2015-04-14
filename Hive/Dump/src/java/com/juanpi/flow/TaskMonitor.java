package com.juanpi.flow;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

public class TaskMonitor {
	public static String querySql = "/opt/project/mail_monitor/task_monitor/setsql.sql";
	public String result          = "/opt/project/mail_monitor/task_monitor/result.txt";
	public String resultLog       = "/opt/project/mail_monitor/task_monitor/result.log";
	
	/**
	 * 对表Rpt_Flow_Task status=2的失败处理进行监控
	 * @param args
	 * 
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
		String data2 = "PROPERTIES_FILE\n" ;
		data2+="---------------------------------------------------------\n" ;
		while(rs.next())
		{
			data += "Hive%20Flow%20Error:"+rs.getString(1)+"%20is%20Error!\n";
			data2 += rs.getString(1)+"\n";
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
		con.close();
	}
	
	public static void main(String[] args) throws Exception {
		TaskMonitor monitor = new TaskMonitor();
		if (args.length > 0) {
			querySql = args[0] ;
		}
		monitor.monitor(querySql);
	}

}
