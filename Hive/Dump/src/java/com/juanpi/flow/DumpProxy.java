package com.juanpi.flow;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.juanpi.flow.exceptions.DataLengthException;
import com.juanpi.tools.Tools;
public class DumpProxy {

	private static Log log= LogFactory.getLog(DumpProxy.class);
	
	private static final int batchSize = 5000; // 由旧版本1000改成5000行提交一次
	// 以下定义2个固定目录
	private static final String SQOOP_DIR = "/user/bi_etl/sqoop_tmp"; // sqoop 临时目录
//	private static String HIVE_CONFIG_HOME = null ;
	private static String INIT = null ;
	
	private Properties prop =null;		
	
	private Map<String, String> externalParam = new HashMap<String,String>();

	public boolean init(String path){
		
		Configuration config=new Configuration();
		boolean success=false;
		success=config.initialize(path);	
		if(success){
			prop = config.getProperties();
//			log.info("overwrite="+prop.getProperty("overwrite")); overwrite
			//如果不是自定义，则使用指定的connection name		
			String overwrite = prop.getProperty("overwrite") ;
			log.info("===========overwrite ===="+overwrite);
			
			if(overwrite==null || "".equals(overwrite) || overwrite.equals("false")){
				log.info("===========get default cofing ====");
//				String connectionName="default";
				//未指定Connection Name, 则使用default
//				if(prop.getProperty("connection.name")!=null &&
//						prop.getProperty("connection.name").trim().length()>0){
//					connectionName=prop.getProperty("connection.name").trim();
//				}				
				useConnection();
			}
			
//			HIVE_CONFIG_HOME=System.getenv("HIVE_CONFIG_HOME");
//			INIT = HIVE_CONFIG_HOME+File.separator+"init.hql" ;
			return true;
		}
		return false;		
	}	
	
	public void useConnection(){	
		Properties conn_prop=new Properties();
		try {
			conn_prop = ConnLoader.getConnection();
			log.info(conn_prop.getProperty("hive.url"));
		} catch (Exception e) {
			log.fatal("Failed to load connection " + ".", e);
			System.exit(Constants.STATUS_INITIAL_FAILED);
		}	  
		prop.setProperty("db.driver", conn_prop.getProperty("db.driver"));		
		prop.setProperty("db.url", conn_prop.getProperty("db.url"));
		prop.setProperty("db.user", conn_prop.getProperty("db.user"));
		prop.setProperty("db.password", conn_prop.getProperty("db.password"));
		prop.setProperty("hive.url", conn_prop.getProperty("hive.url"));
		
	}
	

	public List<String> getListProperties(String key, String sep){
		List<String> stats=new ArrayList<String>();
		String sql = prop.getProperty(key);
		if(sql==null||sql.length()<1){
			return stats;
		}
		for(String tmp:sql.split(sep)){
			stats.add(tmp.trim());			
		}
		return stats;		
	}
	
	public List<String> getListProperties(String key){
		return getListProperties(key, ";");	
	}

	public String getStringProperties(String key){
		return prop.getProperty(key);	
	}
	
	public void setExternalParameter(Map<String,String> parameter){
		this.externalParam = parameter;
	}	
	
	public void exportHive2DB() throws Exception{
		// 首先根据method.dump 判断采用jdbc还是sqoop方式，默认是jdbc方式		
				
		System.setProperty("java.security.egd", "file:///dev/urandom");
		String dumpType = prop.getProperty("method.dump");
		
		if(prop.getProperty("db.url")!=null && prop.getProperty("db.url").contains("mysql")){
			log.info("method.dump is jdbc !");
			jdbcExportHive2Mysql();
			return;
		}
		
		if(dumpType != null && !"".endsWith(dumpType) && "sqoop".equalsIgnoreCase(dumpType))
		{
			log.info("method.dump is Sqoop !");
			sqoopExportHive2DB();
		}else if (dumpType != null && !"".endsWith(dumpType) && "pig".equalsIgnoreCase(dumpType)) {
			// pig 方式
			log.info("method.dump is Pig !");
			pig2DB() ;			
		}
		else{
			log.info("method.dump is jdbc !");
			jdbcExportHive2DB();
		}

	}
	
//	public static void reportExport(String tableName, String taskName, String db_ip, int row_count, long begin_time, long end_time)
//		throws Exception {
//		ConnManager.ConnectionConfig config =ConnManager.getReportDBConfig();
//		Connection conn=Utils.getOracleConnection(config.connURL, config.connUser, EncrypterFactory.getEncryperInstance(1).decode(config.encryptedPassword));
//		Statement stat=conn.createStatement();
//		String sql="insert into rpt_batch_hive_log(table_name, job_name, dest_db, row_count, flag, create_time, begin_time, end_time) " +
//				"values ('" + tableName + "','" + taskName + "','" + db_ip +"'," + row_count + ", 0, to_date('" + 
//				Utils.timeFormat.format(new Date(System.currentTimeMillis()))+"', 'yyyy-mm-dd hh24:mi:ss'), to_date('" +							
//				Utils.timeFormat.format(new Date(begin_time))+"', 'yyyy-mm-dd hh24:mi:ss'), to_date('" +
//				Utils.timeFormat.format(new Date(end_time))	+ "', 'yyyy-mm-dd hh24:mi:ss'))";
//		stat.execute(sql);
//		conn.commit();
//		conn.close();
//	}
	
	
	
	
	public static void main(String[] args) throws Exception {
		DumpProxy dp = new DumpProxy();
		boolean load = false;
	//	if(args!=null && args.length>0){
			load = dp.init("d:\\tmp\\aa.properties");			 
	//	}else{
	//		load = dp.init(null);
	//	}

		//	load = dp.init("conf/DetailPagePV2.properties");
		if(!load){
			log.error("Failed to load the basic configuration, Dump Proxy will exit directly.");
			System.exit(0);
		}		 
		dp.exportHive2DB();	

		
   
		//System.out.println(Utils.timeFormat.format(new java.util.Date()));
//		reportExport("testTable", "testjob", "10.1.0.52", 1, System.currentTimeMillis()-1000000, System.currentTimeMillis());
		

	}	

	public Connection getOracleConnection() throws SQLException {	
		try{
			Class.forName(prop.getProperty("db.driver"));			 
		}catch(Exception ex){
			log.error("Failed to load the DB Driver, " , ex);
			return null;
		}
		Connection con = DriverManager.getConnection(prop.getProperty("db.url"), 
				prop.getProperty("db.user"), EncrypterFactory.getEncryperInstance(1).decode((prop.getProperty("db.password"))));
		return con;
	}

	public Connection getMysqlConnection() throws SQLException {	
		try{
			Class.forName(prop.getProperty("db.driver"));			 
		}catch(Exception ex){
			log.error("Failed to load the DB Driver, " , ex);
			return null;
		}
		String url = prop.getProperty("db.url") ;
		com.mysql.jdbc.Connection con = (com.mysql.jdbc.Connection)DriverManager.getConnection(url, 
				prop.getProperty("db.user"), EncrypterFactory.getEncryperInstance(1).decode((prop.getProperty("db.password"))));
		return con;
	}


	public Connection getHiveConnection() throws Exception{
		String url=prop.getProperty("hive.url");
		if(url.contains("jdbc:hive2")){
			org.apache.hive.jdbc.HiveDriver driver_new=new org.apache.hive.jdbc.HiveDriver();
			DriverManager.registerDriver(driver_new);
		}else{
			org.apache.hadoop.hive.jdbc.HiveDriver driver_old=
					new org.apache.hadoop.hive.jdbc.HiveDriver();
			DriverManager.registerDriver(driver_old);
		}
		Connection con = DriverManager.getConnection(url,"hadoop","123");
		return con;			
	}
	/**
	 * add by lvpeng 2012-3-6
	 * sqoop export 从Hive 到Oracle
	 * @throws Exception 
	 */
	public void sqoopExportHive2DB() throws Exception
	{
		String SQOOP_OPT_DIR= ConnLoader.getSqoopOptDir();
		// 1、Hive执行INSERT OVERWRITE DIRECTORY  把数据导出到HDFS
		//首先拼hive 的insert语句
		String begin_time = Tools.getNow();
		String db_table = prop.getProperty("db.table").trim();
		if (db_table == null || "".equals(db_table)) {
			log.error("db.table cant be null ! ");
			return ;
		}
		String SQOOP_DIR1 = SQOOP_DIR+"/"+db_table+"_"+Math.round(Math.random()*10000000);
		Runtime run = Runtime.getRuntime();
		log.info("hadoop fs -rmr "+SQOOP_DIR1);
		Process p = run.exec("hadoop fs -rmr "+SQOOP_DIR1);
		Connection con1 = getHiveConnection();
		Connection con2 = getOracleConnection();
		//增加根据目的表建数据表  by lvpeng 临时
//		try {
//		String tableNameString = prop.getProperty("db.table") ;
//		if (tableNameString.split("\\.").length==1) {
//			tableNameString = "edw1_user."+tableNameString ;
//		}
//			String mySqlString="create table "+prop.getProperty("db.table")+" as select * from "+tableNameString+"@bi_link where 1=0";
//			log.info(mySqlString);
//			con2.createStatement().execute(mySqlString) ;
//		} catch (Exception e) {
//			log.info("talbe "+ prop.getProperty("db.table") +" is exist !");
//		}
//		
		//end
		
		// 执行预处理
		log.info("Start to preprocess ... ...");
		List<String> preSql= getListProperties("preprocess.sql");
		List<String> preHql= getListProperties("preprocess.hql");
		Utils.batchExecute(con1, preHql, "Hive QL: ", externalParam);
//		boolean isHasParttion = false ;
//		try {
//			if (preSql.size()>0) {
//				for (String s : preSql)
//				{
//					if (s.toUpperCase().contains("PARTITION")) {
//						isHasParttion = true ;
//						// 把s写表
//						con2.createStatement().execute("insert into hadoop_parttion_log(COUNT_DATE,MASSAGE) values ('"+begin_time+"','"+s+"')") ;
//					}
//				}
//			}
//		} catch (Exception e) {
//			log.info("") ;
//		}
//		if (! isHasParttion) {
		Utils.batchExecute(con2, preSql, "SQL: ", externalParam);
//		}
		log.info("Finish the preprocess."); 
		
//		Utils.batchExecute(con2, preSql, "SQL: ", externalParam);
//		log.info("Finish the preprocess."); 
		
		//执行hql
		String hql =prop.getProperty("hive.hql");
		hql = Utils.parseCommand(hql, externalParam);
		String hql_insert = "INSERT OVERWRITE DIRECTORY "+"'"+SQOOP_DIR1+"'  " + hql;

		
		log.info("Running: " + hql_insert);
		//log.info("hive -e \""+hql_insert +"\" -i "+INIT);
		//Process hiveProcess = run.exec("hive -e \""+hql_insert +"\" -i "+INIT); ///opt/project/order_process/aa.sh
		
		//生成sh文件
		StringBuffer sb = new StringBuffer();
		sb.append("#!/bin/bash\n");
		sb.append(". /etc/profile\n");
		sb.append("hive -e \""+hql_insert +"\" -i "+INIT+"\n");
		File sh = new File(SQOOP_OPT_DIR+File.separator+db_table+"-"+Math.round((Math.random()*10000000))+".sh");
		if(! sh.exists())
		{
			try {
				sh.createNewFile();
			} catch (IOException e) {
				System.out.println("Create sqp_opt fail+++++++++++++++++++++++++");
			}
		}
		byte[] b = (sb.toString()).getBytes();
		FileOutputStream fsm = new FileOutputStream(sh);
		fsm.write(b);
		fsm.close();
		//给sh分配可执行权限后执行
		run.exec("chmod +x "+sh.getAbsolutePath()); 
		Process hiveProcess = run.exec("sh "+sh.getAbsolutePath()); 
		//log.info("hiveProcess.waitFor():"+hiveProcess.waitFor()+"==exitValue:"+hiveProcess.exitValue());
		if (hiveProcess.waitFor() != 0) {
			if (hiveProcess.exitValue() == 0)
			{
				log.info("success: " +hql_insert);  
			}
			else {
				throw new RuntimeException("Failed HIVE SQL : " +hql_insert);
			}
	    }
		
		run.exec("hadoop fs -chmod -R 777 "+SQOOP_DIR1);
		
		// 2、从HDFS sqoop export 到Oracle
		File sqp_opt = new File(SQOOP_OPT_DIR+"/tmp_"+db_table+".opt");
		StringBuffer optSb = new StringBuffer();
		optSb.append("export \n");
		optSb.append("--connect\n");
		optSb.append(prop.getProperty("db.url").trim()+"\n");
		optSb.append("--username \n");
		optSb.append(prop.getProperty("db.user").trim()+"\n");
		optSb.append("--password \n");
		optSb.append(EncrypterFactory.getEncryperInstance(1).decode((prop.getProperty("db.password").trim()))+"\n");
		optSb.append("-m\n");
		optSb.append("50\n");
		optSb.append("--input-null-string\n");
		optSb.append("\\\\N\n");
		optSb.append("--input-null-non-string\n");
		optSb.append("\\\\N\n");
		optSb.append("--table\n");
		optSb.append(prop.getProperty("db.table").trim()+"\n");
		String columns = prop.getProperty("db.columns") ;
		if (columns != null && !"".equals(columns)) {
			optSb.append("--columns \n");
			optSb.append(columns + " \n") ;
		}else {
			log.info("Fail: pls set db.columns ! ------------------------------");
		}
		optSb.append("--export-dir \n");
		optSb.append(SQOOP_DIR1+"\n");
		optSb.append("--fields-terminated-by \n");
		optSb.append("'\\001"+"'"+"\n");
		
		// 创建opt文件
		if(! sqp_opt.exists())
		{
			try {
				sqp_opt.createNewFile();
			} catch (IOException e) {
				System.out.println("Create sqp_opt fail+++++++++++++++++++++++++");
			}
		}
		byte[] b2 = (optSb.toString()).getBytes();
		FileOutputStream fs = new FileOutputStream(sqp_opt);
		fs.write(b2);
		fs.close();
		run.exec("chmod 777 "+sqp_opt);
		
		// 执行sqoop 命令
		String shellCmd = "sqoop --options-file "+sqp_opt.getAbsolutePath();
		log.info("sqoop --options-file "+sqp_opt.getAbsolutePath());
		p = run.exec(shellCmd);// 执行sh命令
		if (p.waitFor() != 0) {
			if (p.exitValue() == 1)//p.exitValue()==0表示正常结束，1：非正常结束  
			{
				log.info(readStream(p.getErrorStream()));
				throw new RuntimeException("sqoop --options-file "+sqp_opt.getAbsolutePath() +" error!");
			}
			else {
				log.info("sqoop --options-file "+sqp_opt.getAbsolutePath()+" successful !");
			}
	    }
		//run.exec("hadoop fs -rmr "+SQOOP_DIR1);
		
		log.info("Start to sufix process ... ...");
		List<String> sufSql= getListProperties("sufprocess.sql");
		List<String> sufHql= getListProperties("sufprocess.hql");				
		Utils.batchExecute(con1, sufHql, "Hive QL: ");
		Utils.batchExecute(con2, sufSql, "SQL: ");
		log.info("Finish the sufix process.");	
		
		//获取总行数
		int from = hql.indexOf(" from ");
		String countSql = "select count(*) ttl " + hql.substring(from) ;
		log.info("query count(*) sql:   "+countSql);	
		Statement st = con1.createStatement() ;
		ResultSet rSet = st.executeQuery(countSql) ;
		int line = 0;
		while(rSet.next())
		{
			line = rSet.getInt("ttl") ;
		}
		String dump_type = prop.getProperty("method.dump") ;
		if (dump_type == null || "".equals(dump_type)) {
			dump_type = "jdbc" ;
		}
		String end_time = Tools.getNow();
		String task_name=prop.getProperty("task.name")==null? prop.getProperty("db.table"):prop.getProperty("task.name");
		String db_ip="";
		String url = prop.getProperty("db.url");
		if(url.contains("@")){
			db_ip=url.split("@")[1].split(":")[0];
		}
		if(url.contains("//")){
			db_ip=url.split("//")[1].split(":")[0];
		}		
		Tools.reportExport(prop.getProperty("db.table"),task_name,dump_type, db_ip, line, begin_time, end_time);
		rSet.close();
		st.close();
		con1.close();
		con2.close();	
		log.info("Task execute Finished.");
	}
	
	
	public void pig2DB()
	{ 
		/**
		 * 必须增加属性
		 * export.dir
		 */
		String export_dir = prop.getProperty("export.dir").trim();
		if(export_dir == null || "".equals(export_dir))
		{
			log.info("Error: export.dir  must be set value !");
			System.err.println("Error: export.dir  must be set value !");
			return ;
		}else 
		{
			try {
				this.createOptionsFile(export_dir);
				
				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
	}
	
	public void jdbcExportHive2Mysql() throws Exception{
		String begin_time = Tools.getNow();
		int line=0;
			// jdbc 方式
			// 不设置method.dump的话，默认为jdbc方式	
			// 该部分调整为用select db.columns 代替 select *，如果db.columns 为空，则用select *
		Connection con1 = getHiveConnection();
		Connection con2 = getMysqlConnection();
		log.info("method.dump is jdbc !");
		log.info("Start to preprocess ... ...");
		List<String> preSql= getListProperties("preprocess.sql");
		List<String> preHql= getListProperties("preprocess.hql");
		Utils.batchExecute(con1, preHql, "Hive QL: ", externalParam);
		Utils.batchExecute(con2, preSql, "SQL: ", externalParam);
		log.info("Finish the preprocess."); 

		// show tables
		String hql =prop.getProperty("hive.hql");
		hql = Utils.parseCommand(hql, externalParam);
		log.info("Running: " + hql);	    

		Statement stmt1 = con1.createStatement();		
		ResultSet res = stmt1.executeQuery(hql);		

		Statement stmt2 = con2.createStatement();
		stmt2 = con2.createStatement();	    
		ResultSetMetaData meta2 = null;
		String db_columns = prop.getProperty("db.columns");
		
		if(db_columns == null || "".equals(db_columns))
		{
			meta2 = stmt2.executeQuery("select * from "+ prop.getProperty("db.table")+" where 1=0").getMetaData();
		}
		else
		{
			if (db_columns.startsWith("\"")) {
				db_columns = db_columns.substring(1, db_columns.length()-1);
			}
			meta2 = stmt2.executeQuery("select " +db_columns+ " from "+ prop.getProperty("db.table")+" where 1=0").getMetaData();
		}

		List<String> ignoreColumns = getListProperties("ignore.columns", ",");
		List<String> colName= new ArrayList<String>();
		List<String> colTypeName = new ArrayList<String>();
		List<Integer> colPrecision = new ArrayList<Integer>();
		
		for(int i=0;i<meta2.getColumnCount(); i++){	
			String name = meta2.getColumnName(i+1);
			String type = meta2.getColumnTypeName(i+1);
			int precision = meta2.getPrecision(i+1);
			boolean ignore=false;
			for(String col: ignoreColumns){
				if(name.equalsIgnoreCase(col)){
					ignore = true;
					break;
				}				
			}
			if(!ignore){
				colName.add(name);
				colTypeName.add(type);
				colPrecision.add(precision);
			}						
		}		
		
		StringBuilder sql=new StringBuilder("insert into "+prop.getProperty("db.table")+ " (");	

		String prefix="";
		for(int i=0;i<colName.size(); i++){	
			sql.append(prefix+ colName.get(i));
			if(i==0){
				prefix=", ";
			}						
		}
		sql.append(") values (");

		prefix ="";

		for(int i=0;i<colName.size();i++){
			sql.append(prefix + "?");
			if(i==0){
				prefix = ",";
			}			
		}
		sql.append(")");
		log.info(sql);

		log.info(" Start to transfer data...");		
		PreparedStatement pstmt = con2.prepareStatement(sql.toString());	    
		
		while(res.next()){
			log.debug("\nLineNum: "+ (line++));			
			for(int i=0; i< colName.size();i++){
				String str = res.getString(i+1);
				if("null".equalsIgnoreCase(str)){
					pstmt.setString(i+1, null);
					continue;
				}
				//System.out.println(colTypeName.get(i));
				if(colTypeName.get(i).equals("VARCHAR") && colPrecision.get(i)>0){
					pstmt.setString(i+1, StringEscape.validate(str, colPrecision.get(i), StringEscape.DB_STRING_COLUMN.MYSQL_VARCHAR));
				}else{
					pstmt.setString(i+1, StringEscape.validate(str));
				}
			}
			
			pstmt.addBatch();			
			if(line%batchSize==0){	
				pstmt.executeBatch();				
				log.info("Currently: "+ line + " lines has been inserted successfully!");
				pstmt.clearBatch();
			}
		}
		if(line%batchSize!=0){
			pstmt.executeBatch();
		}
		
		log.info(" Data Transfer Finished: "+ line + " lines has been transferred!");

		log.info("Start to sufix process ... ...");
		List<String> sufSql= getListProperties("sufprocess.sql");
		List<String> sufHql= getListProperties("sufprocess.hql");				
		Utils.batchExecute(con1, sufHql, "Hive QL: ", externalParam);
		Utils.batchExecute(con2, sufSql, "SQL: ", externalParam);
		log.info("Finish the sufix process.");		
			
		log.info("Task execute successfully.");
	
		String end_time = Tools.getNow();
		String dump_type = prop.getProperty("method.dump") ;
		if (dump_type == null || "".equals(dump_type)) {
			dump_type = "jdbc" ;
		}
		String task_name=prop.getProperty("task.name")==null? prop.getProperty("db.table"):prop.getProperty("task.name");
		String db_ip="";
		String url = prop.getProperty("db.url");
		if(url.contains("@")){
			db_ip=url.split("@")[1].split(":")[0];
		}
		if(url.contains("//")){
			db_ip=url.split("//")[1].split(":")[0];
		}
		Tools.reportExport(prop.getProperty("db.table"),task_name,dump_type, db_ip, line, begin_time, end_time);	
		res.close();
		stmt1.close();
		con1.close();
		stmt2.close();
		con2.close();	
	}	
	
	
	public void jdbcExportHive2DB() throws Exception{
		String begin_time = Tools.getNow();
		int line=0;
			// jdbc 方式
			// 不设置method.dump的话，默认为jdbc方式	
			// 该部分调整为用select db.columns 代替 select *，如果db.columns 为空，则用select *
		Connection con1 = getHiveConnection();
		Connection con2 = getOracleConnection();
		
		//增加根据目的表建数据表  by lvpeng 临时
//		try {
//		String tableNameString = prop.getProperty("db.table") ;
//		if (tableNameString.split("\\.").length==1) {
//			tableNameString = "edw1_user."+tableNameString ;
//		}
//			String mySqlString="create table "+prop.getProperty("db.table")+" as select * from "+tableNameString+"@bi_link where 1=0";
//			log.info(mySqlString);
//			con2.createStatement().execute(mySqlString) ;
//		} catch (Exception e) {
//			log.info("talbe "+ prop.getProperty("db.table") +" is exist !");
//		}
		//end
		
		log.info("Start to preprocess ... ...");
		List<String> preSql= getListProperties("preprocess.sql");
		List<String> preHql= getListProperties("preprocess.hql");
		Utils.batchExecute(con1, preHql, "Hive QL: ", externalParam);
//		boolean isHasParttion = false ;
//		try {
//			if (preSql.size()>0) {
//				for (String s : preSql)
//				{
//					if (s.toUpperCase().contains("PARTITION")) {
//						isHasParttion = true ;
//						// 把s写表
//						con2.createStatement().execute("insert into hadoop_parttion_log(COUNT_DATE,MASSAGE) values ('"+begin_time+"','"+s+"')") ;
//					}
//				}
//			}
//		} catch (Exception e) {
//			log.info("") ;
//		}
//		if (! isHasParttion) {
		Utils.batchExecute(con2, preSql, "SQL: ", externalParam);
//		}
		log.info("Finish the preprocess."); 

		// show tables
		String hql =prop.getProperty("hive.hql");
		hql = Utils.parseCommand(hql, externalParam);
		log.info("Running: " + hql);	    

		Statement stmt1 = con1.createStatement();	
		
		//此处使用适配器处理新集群的bug
//		ResultSetAdapter res = new ResultSetAdapter(stmt1.executeQuery(hql));		
		ResultSet res = stmt1.executeQuery(hql);	
		
		Statement stmt2 = con2.createStatement();
//		stmt2 = con2.createStatement();	    
		ResultSetMetaData meta2 = null;
		String db_columns = prop.getProperty("db.columns");
		
		if(db_columns == null || "".equals(db_columns))
		{
			meta2 = stmt2.executeQuery("select * from "+ prop.getProperty("db.table")+" where 1=0").getMetaData();
		}
		else
		{
			if (db_columns.startsWith("\"")) {
				db_columns = db_columns.substring(1, db_columns.length()-1);
			}
			meta2 = stmt2.executeQuery("select " +db_columns+ " from "+ prop.getProperty("db.table")+" where 1=0").getMetaData();
		}

		List<String> ignoreColumns = getListProperties("ignore.columns", ",");
		List<String> colName= new ArrayList<String>();
		List<String> colTypeName = new ArrayList<String>();
		List<Integer> colPrecision = new ArrayList<Integer>();
		
		for(int i=0;i<meta2.getColumnCount(); i++){	
			String name = meta2.getColumnName(i+1);
			String type = meta2.getColumnTypeName(i+1);
			int precision = meta2.getPrecision(i+1);
			boolean ignore=false;
			for(String col: ignoreColumns){
				if(name.equalsIgnoreCase(col)){
					ignore = true;
					break;
				}				
			}
			if(!ignore){
				colName.add(name);
				colTypeName.add(type);
				colPrecision.add(precision);
			}						
		}		
		
		StringBuilder sql=new StringBuilder("insert into "+prop.getProperty("db.table")+ " (");	

		String prefix="";
		for(int i=0;i<colName.size(); i++){	
			sql.append(prefix+ colName.get(i));
			if(i==0){
				prefix=", ";
			}						
		}
		sql.append(") values (");

		prefix ="";

		for(int i=0;i<colName.size();i++){
			sql.append(prefix + "?");
			if(i==0){
				prefix = ",";
			}			
		}
		sql.append(")");
		log.info(sql);

		log.info(" Start to transfer data...");		
		SimpleDateFormat dateFormat3 = new SimpleDateFormat("yyyy-MM-dd");
		PreparedStatement pstmt = con2.prepareStatement(sql.toString());	    
		
		while(res.next()){
			log.debug("\nLineNum: "+ (line++));			
			for(int i=0; i< colName.size();i++){
				if(colTypeName.get(i).equalsIgnoreCase("NUMBER")){
					if(colPrecision.get(i)==0){ //for unknown precision
						colPrecision.set(i, 18); //because long only support 10^18;
					}					
					if(meta2.getScale(i+1)==0 && res.getLong(i+1)>=Math.pow(10, colPrecision.get(i))){
						throw new DataLengthException(colName.get(i), colPrecision.get(i), res.getString(i+1));
					}					
					if(meta2.getScale(i+1)>0){
						pstmt.setDouble(i+1, res.getDouble(i+1));
					}else if( colPrecision.get(i)>9){	
						pstmt.setLong(i+1, res.getLong(i+1));
					}else{
						pstmt.setInt(i+1, res.getInt(i+1));
					}					
				}else if(colTypeName.get(i).equalsIgnoreCase("DATE")){ 
					String time = res.getString(i+1);					
					try{						
						if(time==null || time.trim().equalsIgnoreCase("null")){
							//log.warn("----0  "+time);
							pstmt.setDate(i+1, null);
						}else if(time.trim().length()>=12){
							//log.warn("----1  "+time);
							pstmt.setTimestamp(i+1, Timestamp.valueOf(res.getString(i+1)));
						}else{
							//log.warn("----2  "+time);
							pstmt.setDate(i+1, new Date(dateFormat3.parse(res.getString(i+1)).getTime()));
						}
					}catch (Exception ex){
						log.warn("Failed to recognize the value "+ res.getString(i+1) +". Ignored it.");
						pstmt.setDate(i+1, null);	
					}					
				}else{
					String str = res.getString(i+1);
					if("null".equalsIgnoreCase(str)){
						pstmt.setString(i+1, null);
					}else{
						if(colPrecision.get(i)>0 && "VARCHAR2".equalsIgnoreCase(colTypeName.get(i))){
							pstmt.setString(i+1, StringEscape.validate(str,  colPrecision.get(i), StringEscape.DB_STRING_COLUMN.ORACLE_VARCHAR2));
						}else{
							pstmt.setString(i+1, StringEscape.validate(str));
						}						
					}
				}
			}
			
			pstmt.addBatch();			
			if(line%batchSize==0){	
				pstmt.executeBatch();				
				log.info("Currently: "+ line + " lines has been inserted successfully!");
				pstmt.clearBatch();
			}
		}
		if(line%batchSize!=0){
			pstmt.executeBatch();
		}
		
		log.info(" Data Transfer Finished: "+ line + " lines has been transferred!");

		log.info("Start to sufix process ... ...");
		List<String> sufSql= getListProperties("sufprocess.sql");
		List<String> sufHql= getListProperties("sufprocess.hql");				
		Utils.batchExecute(con1, sufHql, "Hive QL: ", externalParam);
		Utils.batchExecute(con2, sufSql, "SQL: ", externalParam);
		log.info("Finish the sufix process.");		
			
		log.info("Task execute successfully.");
		String dump_type = prop.getProperty("method.dump") ;
		if (dump_type == null || "".equals(dump_type)) {
			dump_type = "jdbc" ;
		}
		String end_time = Tools.getNow();
		String task_name=prop.getProperty("task.name")==null? prop.getProperty("db.table"):prop.getProperty("task.name");
		String db_ip="";
		String url = prop.getProperty("db.url");
		if(url.contains("@")){
			db_ip=url.split("@")[1].split(":")[0];
		}
		if(url.contains("//")){
			db_ip=url.split("//")[1].split(":")[0];
		}
		Tools.reportExport(prop.getProperty("db.table"),task_name,dump_type, db_ip, line, begin_time, end_time);	
		res.close();
		stmt1.close();
		con1.close();
		stmt2.close();
		con2.close();	
	}	
	
	
	
	
	public void createOptionsFile(String export_dir) throws Exception
	{
		String SQOOP_OPT_DIR= ConnLoader.getSqoopOptDir();
		File sqp_opt = new File(SQOOP_OPT_DIR+"/tmp_"+prop.getProperty("db.table").trim()+".opt");
		StringBuffer optSb = new StringBuffer();
		optSb.append("export \n");
		optSb.append("--connect\n");
		optSb.append(prop.getProperty("db.url").trim()+"\n");
		optSb.append("--username \n");
		optSb.append(prop.getProperty("db.user").trim()+"\n");
		optSb.append("--password \n");
		optSb.append(EncrypterFactory.getEncryperInstance(1).decode((prop.getProperty("db.password").trim()))+"\n");
		optSb.append("-m\n");
		optSb.append("1\n");
		optSb.append("--table \n");
		optSb.append(prop.getProperty("db.table").trim()+"\n");
		optSb.append("--export-dir \n");
		optSb.append(export_dir+"\n");
		optSb.append("--fields-terminated-by \n");
		optSb.append("'\\001"+"'"+"\n");
		
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
	}
	public static String readStream(InputStream inStream) throws Exception {  
	  int count = inStream.available();
	  byte[] b = new byte[count];
	  inStream.read(b);
	  return new String(b);
	} 
	
	
	
}
