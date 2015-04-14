package com.juanpi.flow;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * 该类用于适配CDH4中JDBC的 Bug HIVE-2381， 详情见：https://issues.apache.org/jira/browse/HIVE-2381 
 * @author zhangyongliang
 *
 */

public class ResultSetAdapter {
	
	private ResultSet  rs =null;
	
	public ResultSetAdapter(ResultSet res){
		this.rs= res;
	}
	
 
	public boolean next(){
		boolean hasNext=false;
		try{
			hasNext=rs.next();
		}catch(Exception ex){
			//used to catch the exception thrown by bug HIVE-2381
		}
		return hasNext;		
	}	
	
	
	public Double getDouble(int index) throws SQLException{
		return rs.getDouble(index);	
	}
	
	public Integer getInt(int index) throws SQLException{
		return rs.getInt(index);	
	}
	
	public Long getLong(int index) throws SQLException{
		return rs.getLong(index);	
	}
	
	public String getString(int index) throws SQLException{
		return rs.getString(index);	
	}
	
	public void close() throws SQLException{
		rs.close();	
	}

}
