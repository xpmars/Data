package com.juanpi.tools;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.log4j.Logger;

import com.juanpi.flow.DBConnection;

public class Del_Hive_History {

	/**
	 * 该类读取hadoop_del_daily 的配置，清理Hadoop老集群hive表历史数据
	 * @param args
	 */
//	public static Logger log = Logger.getLogger(Del_Hive_History.class);
	public Connection con = null ;
	public Statement  sta = null ;
	public ResultSet  rst = null ;
	
	private Integer task_id = null ;
	private String hive_table = null ;
	private String location = null ;
	private String ds = null ;
	private Integer save_days = null ;
	
	private String count_date = null;
	SimpleDateFormat sft = new SimpleDateFormat("yyyy-MM-dd"); 
	
	public void del_history(String[] args)
	{
		try {
			String sql = "select * from hadoop_del_daily where is_use='Y' " ;
			if (args.length>=1) {
				sql += "and task_id="+args[0];
			}
			if (args.length==2) {
				count_date = args[1] ;
			}
			sql += "  order by task_id" ;
			con = DBConnection.getInstance() ;
			sta = con.createStatement() ;
			rst = sta.executeQuery(sql);
			String hql = null;
			String dfs = null;
//			Runtime run = Runtime.getRuntime();
			while(rst.next())
			{
				this.setResult(rst,args);
				hql = "hive -e \"ALTER TABLE "+hive_table+" DROP PARTITION (ds='"+count_date+"');\"" ;
				System.out.println(hql);
				try {
//					run.exec(hql);
				} catch (Exception e) {
					e.printStackTrace();
				}
				//eg:     /user/hive/warehouse/rpt_keyword_prdt_show_daily/ds=2012-09-01
				dfs = "hadoop fs -rmr "+location+"/"+ds;
				System.out.println(dfs);
				try {
//					run.exec(dfs);
				} catch (Exception e) {
					e.printStackTrace();
				}
				
			}
			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			try {
				rst.close() ;
				sta.close() ; 
				con.close() ;
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}
	
	public void setResult(ResultSet rs,String[] args) throws SQLException
	{
		task_id = rs.getInt("TASK_ID") ;
		hive_table = rs.getString("HIVE_TABLE");
		location = rs.getString("LOCATION");
		ds = rs.getString("DS");
		save_days = rs.getInt("SAVE_DAYS") ;
		
		//count_date 
		Calendar v_date = Calendar.getInstance() ;
		v_date.add(Calendar.DAY_OF_MONTH, -save_days);
		if (args.length<2) {
			count_date = sft.format(v_date.getTime()) ;
		}
		//对DS进行格式处理
		ds = ds.replaceAll("\\$TXDATE", count_date) ;
	}
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		Del_Hive_History aHistory = new Del_Hive_History();
		aHistory.del_history(args);


	}

}
