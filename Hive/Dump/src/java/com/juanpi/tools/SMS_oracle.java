package com.juanpi.tools;

import java.sql.Connection;
import java.sql.Statement;

import com.juanpi.flow.DBConnection;

public class SMS_oracle {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String mobiles = args[0] ;
		String content = args[1] ;
		try {
			Connection con = DBConnection.getInstance();
			Statement st = con.createStatement() ;
			
			if (con == null) {
				con = DBConnection.getInstance();
			}
			if(st == null){
				st = con.createStatement();
			}
			String sql = "insert into tb_queue@edm_link(id,phone,msg,pwd,inserttime,sendlevel,svrtype,smstotal,jobid) " ;
			sql += " values (EDM_USER.SEQ_QUE.NEXTVAL@edm_link,'"+mobiles+"','"+content+"',0,sysdate,2,14,0,0)" ;
			st.execute(sql) ;
			
			st.close() ;
			con.close() ;
		} catch (Exception e) {
			e.printStackTrace() ;
		}
	}

}
