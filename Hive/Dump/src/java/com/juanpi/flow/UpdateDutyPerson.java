package com.juanpi.flow;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class UpdateDutyPerson {

	/**
	 * @param args
	 */
	public final String normal = "18602165085,13816776472," ;
	public final String mobile_cfg = "/opt/project/mail_monitor/mobile.cfg" ;
	public void updateDutyPerson() throws Exception
	{
		StringBuffer sBuffer = new StringBuffer();
		sBuffer.append(normal);
		Calendar tomorrow = Calendar.getInstance() ;
		SimpleDateFormat sdFormat = new SimpleDateFormat("yyyy-MM-dd");
		tomorrow.add(Calendar.DAY_OF_MONTH, 1) ;
		System.out.println(sdFormat.format(tomorrow.getTime()));
		String sql = "select MOBILE from hadoop_duty  where count_date=date'"+sdFormat.format(tomorrow.getTime())+"'" ;
		Connection con = DBConnection.getInstance();
		Statement st   = con.createStatement() ;
		ResultSet rs = st.executeQuery(sql) ;
		while (rs.next()) {
//			System.out.println(rs.getDate(1));
//			System.out.println(rs.getString(2));
//			System.out.println(rs.getString(3));
			sBuffer.append(rs.getString("MOBILE")+",") ;
		}
		
		File file = new File(mobile_cfg) ;
		if(! file.exists())
		{
			try {
				file.createNewFile();
			} catch (IOException e) {
				System.out.println("Create file fail+++++++++++++++++++++++++");
			}
		}
		byte[] b = (sBuffer.toString()).getBytes();
		FileOutputStream fs = new FileOutputStream(file);
		fs.write(b);
		fs.close();
		
		rs.close();
		st.close();
		con.close();
	}
	
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		new UpdateDutyPerson().updateDutyPerson();
	}

}
