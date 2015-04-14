package com.juanpi.flow;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;

import org.apache.log4j.Logger;

public class EtlMain {

	/**
	 * @param args
	 * 
	 * 支持：
	 * Flow etl -config /opt/aa.properties -task 3 "2014-09-01"
	 */
	public static Logger log = Logger.getLogger(EtlMain.class);
	public static void main(String[] argc) {
		
		Connection con = null;
		Statement st = null;
		ResultSet rs = null;
		String taskid = null;
		SimpleDateFormat sfmt = new SimpleDateFormat("yyyy-MM-dd");
		Calendar calendar = Calendar.getInstance() ;
		calendar.add(Calendar.DAY_OF_MONTH, -1) ;
		//count_date 默认为yestoday
		String count_date = sfmt.format(calendar.getTime());
		if (argc.length == 0) {
			return ;
		}
		List<String> argList = Arrays.asList(argc) ;
//		argc
		//含-config 
		if (argList.contains("-config")) {
			//Flow etl -config /opt/aa.properties 2014-08-10
			if (argc.length == 4) {
				count_date = argc[3];
			}
			//Flow etl -config /opt/aa.properties -task 3 "2014-09-01"
			if (argc.length >= 5 && argc[3].equalsIgnoreCase("-task")) {
				taskid = argc[4] ;
				if (argc.length > 5) {
					count_date = argc[5] ;
				}
			}
			try {
				con = DBConnection.getInstance(argc[2]);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		else {
			// Flow etl "2013-08-01"
			if (argc.length == 2) {
				count_date = argc[1];
			}
			//Flow etl -task 3 "2013-08-01"
			if (argc.length >= 3 && argc[1].equalsIgnoreCase("-task")) {
				taskid = argc[2];
				if (argc.length > 3) {
					count_date = argc[3];
				}
			}
			try {
				con = DBConnection.getEtlCon();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		try {
			
			st = con.createStatement();
		} catch (Exception e) {
			e.printStackTrace();
		}
		String sql = "select t.*,d.uname,d.password,d.url from bi_extract_to_hdfs t,bi_extract_database_info d where t.database_id = d.id " ;
		if (taskid != null) {
			sql += " and task_id="+Integer.parseInt(taskid);
		}else {
			sql += " and t.is_use='Y' ";
		}
		sql += " order by t.priority" ;
		log.info("sql: "+sql);
		
		ArrayList<ETLModel> list = new ArrayList<ETLModel>();
		try {
			rs = st.executeQuery(sql);
			int num = 0;
			while (rs.next()) {
				ETLModel model = new ETLModel();
				model.setCount_date(count_date);
				model.setTask_id(rs.getInt("task_id"));
				model.setDb_table(rs.getString("db_table").trim());
				model.setDb_colunms(rs.getString("db_columns").trim());
				
				model.setWhere(rs.getString("where_express"));
				model.setTarget_dir(rs.getString("target_dir"));
				model.setIs_hive_import(rs.getString("is_hive_import"));
				model.setHive_table(rs.getString("hive_table"));
				
				model.setHive_pt_key(rs.getString("hive_partition_key"));
				model.setIs_use(rs.getString("is_use"));
				model.setMappers(rs.getInt("mappers"));
				model.setSplit_by(rs.getString("split_by"));
				
				model.setDb_userName(rs.getString("uname").trim());
				model.setDb_passwd(rs.getString("password").trim());
				model.setDb_url(rs.getString("url").trim());
//				model.setIs_col_upper(rs.getString("IS_COLUMN_UPPER"));
				list.add(model);
				num ++ ;
				if (num % 10 == 0) {
					// 每隔5个
					ArrayList<ETLModel> oneList  = new ArrayList<ETLModel>(list);
					Runnable r1 = new ETL(oneList) ;
					Thread t1 = new Thread(r1);
					t1.start() ;
					log.info("start one trhead for etl .") ;
					// 清空list
					list.clear() ;
				}
			}
			if (list.size() > 0) {
				Runnable r1 = new ETL(list) ;
				Thread t1 = new Thread(r1);
				t1.start() ;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		finally
		{
			try {
				rs.close();
				st.close();
//				con.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			
		}
		
	}

}
