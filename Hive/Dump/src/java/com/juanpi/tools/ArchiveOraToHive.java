package com.juanpi.tools;

import com.juanpi.flow.ConnLoader;
import com.juanpi.flow.DBConnection;
import com.juanpi.flow.ETL_vo;
import com.juanpi.flow.EncrypterFactory;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;

public class ArchiveOraToHive {

	/**
	 * @param args
	 * 该类负责管理HDFS的数据抽取工作
	 * 参数介绍
	 *  args[0] = "archive" ;
	 *	args[1] = "1" ;
	 *	args[2] = "2013-04-01" ;
	 * 后两个可选
	 *	args[3] = "-task" ;
	 *	args[4] = "10" ;
	 */
	public static Logger log = Logger.getLogger(ArchiveOraToHive.class);
	private static String SQOOP_OPT_DIR = "" ; // sqoop option目录 c:/test   /opt/temp/tmp_sqoop_opt  
	
	private Connection con = null;
	private Statement st = null;
	private ResultSet rs = null;
	
	private Connection con2 = null;
	private Statement st2 = null;
	private ResultSet rset2 = null;
	
	private String count_date = null;
	private String OFFSET = null ;
	private int	task_id   ;
	private String	db_user             = null;
	private String	db_password         = null;
        
	private String	db_table            = null;
	private String	db_columns          = null;
	private String	url                 = null;
	private String	where               = null;
	private String	target_dir          = null;
        
	private String	is_hive_import      = null;
	private String	hive_table          = null;
	private String	hive_partition_key  = null;
	private String	hive_partition_value= null;
	private String split_by             = null;
	private String	mappers= null;
	
	private String min_data_date        = null;
	private final String oracle_driver = "oracle.jdbc.driver.OracleDriver" ;
	
	File sqp_opt = null;
	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	SimpleDateFormat sfmt = new SimpleDateFormat("yyyy-MM-dd");
	public void extract(String []argc)
	{
		SQOOP_OPT_DIR= ConnLoader.getSqoopOptDir();		
		// 1 读取配置表内容 yhd_extract_to_hdfs
		if (argc.length == 2) {
			log.info("args is not enough!") ;
			return ;
		}
		if (argc.length == 3) {
			OFFSET = argc[1] ;
			count_date = argc[2] ;
			hive_partition_value = count_date ;
		}
		try {
			int taskid = 0;
			if (argc.length == 5 && argc[3].equalsIgnoreCase("-task")) {
				try {
					OFFSET = argc[1] ;
					count_date = argc[2] ;
					hive_partition_value = count_date ;
					taskid = Integer.parseInt(argc[4]);
				} catch (Exception e) {
					log.info("main args is wrong , pls check !");
					return;
				}
			}
			con = DBConnection.getInstance();
			st = con.createStatement();
			String sql = "select t.*,d.uname,d.password,d.url from yhd_extract_to_archivedb t,yhd_extract_database_info d where t.database_id = d.id " ;
			if (taskid > 0) {
				sql += " and task_id="+taskid;
			}else {
				sql += " and t.is_use='Y' ";
			}
			sql += " order by t.priority" ;
			log.info("sql: "+sql);
			rs = st.executeQuery(sql);
			Runtime run = null;
			List<ETL_vo> list = new ArrayList<ETL_vo>();
			int num = 0;
			while(rs.next())
			{
				ETL_vo vo = new ETL_vo();
				setFromResultSet(rs);
				
				//判断时间，是否Oracle表有数据可备份
				if (min_data_date != null && !"".equals(min_data_date) 
						&& sfmt.parse(min_data_date).after(sfmt.parse(hive_partition_value))) {
					continue ;
				}
				vo.setBkDate(hive_partition_value);
				vo.setTableName(hive_table);
				int flag = this.createOptionsFile();
				if (flag == -1) {
					log.info("Error: create option file fail, please check yhd_extract_to_hdfs table ! task_id 为"+task_id +"    "+sdf.format(new Date())) ;
					continue ;
				}else {
					//增加判断，如果该表无数据，则不抽取
					if (!this.isHasData(oracle_driver,db_user,db_password,url,db_table,where)) {
						continue ;
					}
					
					run = Runtime.getRuntime();
					try {
						if (target_dir != null && ! "".equals(target_dir)) {
							run.exec("hadoop fs -rmr "+target_dir);
						}else {
							run.exec("hadoop fs -rmr "+db_table.trim());
							run.exec("hadoop fs -rmr "+db_table.trim().toUpperCase());
							run.exec("hadoop fs -rmr "+db_table.trim().toLowerCase());
						}
						log.info("Begin excuete task "+task_id+" "+hive_table+" ......"+sdf.format(new Date())) ;
						vo.setBeginTime(sdf.format(new Date()));
						vo.setResult("SUCCESS");
						Process p = run.exec("sqoop --options-file "+sqp_opt.getAbsolutePath());
						if (p.waitFor() != 0) {
							if (p.exitValue() == 1)//p.exitValue()==0表示正常结束，1：非正常结束  
							{
								System.err.println("Error: sqoop --options-file "+sqp_opt.getAbsolutePath() +" error! "+sdf.format(new Date())) ;
								log.info("Error: sqoop --options-file "+sqp_opt.getAbsolutePath() +" error! "+sdf.format(new Date())) ;
								vo.setEndTime(sdf.format(new Date()));
								vo.setResult("FAIL");
							}else {
								vo.setResult("SUCCESS");
							}
					    }
						num ++ ;
						vo.setEndTime(sdf.format(new Date()));
					} catch (Exception e) {
						log.info("Error: Job fail task_id is "+task_id +"    "+sdf.format(new Date())) ;
						vo.setEndTime(sdf.format(new Date()));
						vo.setResult("FAIL");
						continue;
					}
				}
				list.add(vo);
			}
			log.info("Finish Extracting ! "+sdf.format(new Date())) ;
			if (num>0) {
				saveResultReport(list);
			}
		} catch (Exception e) {
			log.info("Error: extract fail !");
			e.printStackTrace();
			return ;
		}
		finally{
			try {
				rs.close();
				st.close();
				con.close();
				rset2.close();
				st2.close();
				con2.close() ;
			} catch (SQLException e) {
				e.printStackTrace();
			}
			
		}
	}
	
	public void setFromResultSet(ResultSet rs) throws SQLException
	{
		task_id             = rs.getInt("task_id");
		db_user             = rs.getString("uname").trim();
		db_password         = rs.getString("password").trim();
		                    
		db_table            = rs.getString("db_table").trim();
		url                 = rs.getString("url");
		db_columns          = rs.getString("db_columns");
		where               = rs.getString("where_express");
		target_dir          = rs.getString("target_dir");
		                    
		is_hive_import      = rs.getString("is_hive_import");
		hive_table          = rs.getString("hive_table");
		hive_partition_key  = rs.getString("hive_partition_key");
		mappers              = rs.getString("mappers"); 
		split_by             = rs.getString("split_by"); 
		min_data_date       = rs.getString("MIN_DATA_DATE"); 
	}
	public int createOptionsFile() throws Exception
	{
		int result = 0;
		sqp_opt = new File(SQOOP_OPT_DIR+"/tmp_"+db_table+".opt");
		StringBuffer optSb = new StringBuffer();
		optSb.append("import \n");
		optSb.append("--connect\n");
		optSb.append(url+"\n");
		optSb.append("--username \n");
		optSb.append(db_user+"\n");
		optSb.append("--password \n");
		optSb.append(EncrypterFactory.getEncryperInstance(1).decode(db_password)+"\n");
		if (mappers != null && ! "".equals(mappers)) 
		{  
			optSb.append("-m\n");
			optSb.append(mappers+"\n");
		}
		if (split_by != null && !"".equals(split_by)) {
			optSb.append("--split-by\n");
			optSb.append(split_by.toUpperCase()+"\n");
		}
		
		optSb.append("--null-string\n");
		optSb.append("''\n");
		
		optSb.append("--table \n");
		optSb.append(db_table.toUpperCase()+"\n");
		//以下是数据表非必填项，需要判断
		if(db_columns != null && ! "".equals(db_columns)){
			optSb.append("--columns \n");
			optSb.append("\""+db_columns.trim().toUpperCase()+"\"\n");
		}
		if (where != null && ! "".equals(where)) {
			optSb.append("--where \n");
			where = where.replaceAll("\\$OFFSET", OFFSET) ;
			where = where.replaceAll("\\$TXDATE", "'"+count_date+"'") ;
			where = where.replaceAll("\\$DATEID", count_date.replaceAll("\\-", "")) ;  //处理日期number类型
			optSb.append("\""+where.trim()+"\"\n");
		}
		
		if (target_dir != null && ! "".equals(target_dir)) {
			optSb.append("--target-dir \n");
			optSb.append(target_dir.trim()+"\n");
		}
		
		if (is_hive_import != null && ! "".equals(is_hive_import)
				&& "Y".equalsIgnoreCase(is_hive_import))
		{
			optSb.append("--hive-overwrite \n");  // 统一为覆盖模式，要求hive 表必须存在
			optSb.append("--hive-import \n");
			optSb.append("--hive-drop-import-delims\n");
			
			if (hive_table == null || "".equals(hive_table)) {
				log.info("Error: hive_table must be set 当--hive-import时 !");
				result = -1 ;
			}else {
				optSb.append("--hive-table \n");
				optSb.append(hive_table.trim()+"\n");
			}
			if (hive_partition_key != null && !"".equals(hive_partition_key)) {
				optSb.append("--hive-partition-key \n");
				optSb.append(hive_partition_key.trim()+"\n");
				optSb.append("--hive-partition-value \n");
				optSb.append("\""+hive_partition_value.trim()+"\"\n");
			}
		}
		
		if(! sqp_opt.exists())
		{
			try {
				sqp_opt.createNewFile();
			} catch (IOException e) {
				System.out.println("Create sqp_opt fail+++++++++++++++++++++++++");
			}
		}
		byte[] b = (optSb.toString()).getBytes();
		FileOutputStream fs = new FileOutputStream(sqp_opt);
		fs.write(b);
		fs.close();
		return result;
	}
	
	public void saveResultReport(List<ETL_vo> list) throws Exception
	{
		if (con == null) {
			con = DBConnection.getInstance();
		}
		if(st == null){
			st = con.createStatement();
		}
		for (ETL_vo vo : list) {
			String sql = "insert into hadoop_etl_archive values('"+vo.getTableName()+"','"+vo.getBeginTime()+"','"+vo.getEndTime()+"','"+vo.getResult()+"','"+vo.getBkDate()+"')";
			//log.info(sql);
			st.execute(sql);
		}
	}
	public boolean isHasData(String driver,String user,String passwd,String url,String oracle_table,String where) throws Exception 
	{
		Class.forName(driver);
		con2 = DriverManager.getConnection(url, user, 
				EncrypterFactory.getEncryperInstance(1).decode(passwd));
		st2 = con2.createStatement() ;
		String sql = "select * from "+oracle_table+" where "+ where ;
		log.info("sql: "+sql);
		rset2 = st2.executeQuery(sql) ;
		while (rset2.next()) {
			return true; 
		}
		return false ;
	}
	
	public static void main(String[] args) {
//		args[0] = "archive" ;
//		args[1] = "1" ;
//		args[2] = "2013-04-01" ;
//		args[3] = "-task" ;
//		args[4] = "10" ;
		ArchiveOraToHive archive = new ArchiveOraToHive();
		archive.extract(args) ;
		
	}

}
