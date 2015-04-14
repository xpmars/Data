package com.juanpi.tools;

import com.juanpi.flow.Flow;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import com.juanpi.flow.DBConnection;

public class CreateHiveTableFromDB {
	private Connection con = null;
	private Statement st = null;
	private ResultSet rs = null;
	public CreateHiveTableFromDB()
	{
		try {
			con = DBConnection.getInstance() ;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		CreateHiveTableFromDB create = new CreateHiveTableFromDB();
		create.createHiveTable() ;
		
	}
	
	public void createHiveTable()
	{
		
		try {
			String sql = "select * from yhd_extract_to_archivedb where is_use='Y' order by task_id" ;
			st = con.createStatement() ; 
			rs = st.executeQuery(sql) ;
			while (rs.next())
			{
//				Flow.printCreateStatement(rs.getString("DB_TABLE"), "archivedb."+rs.getString("HIVE_TABLE"), rs.getString("HIVE_PARTITION_KEY")==null?1:0, rs.getString("DATABASE_ID")) ;
			}
			
			rs.close();
			st.close();
			con.close() ;
			
			
			
		} catch (Exception e) {
			e.printStackTrace() ;
		}
	}
}
		

