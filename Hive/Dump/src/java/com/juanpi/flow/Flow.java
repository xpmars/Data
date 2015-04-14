package com.juanpi.flow;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

import oracle.jdbc.OracleDriver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.juanpi.tools.ArchiveOraToHive;



public class Flow {	

	private static Log log= LogFactory.getLog(Flow.class);

	public static String getSchema(String table) throws Exception{
//		Connection conn=Utils.getOracleConnection(db);
		Connection conn=DBConnection.getInstance(null) ;
		Statement stmt = conn.createStatement();	   
		ResultSetMetaData meta2 = stmt.executeQuery("select * from " + table +" where 1=0").getMetaData();
		StringBuilder sql=new StringBuilder();
		for(int i=0;i<meta2.getColumnCount();i++){
			sql.append(meta2.getColumnName(i+1)+" ");
			System.out.println(meta2.getColumnTypeName(i+1));
			if(!meta2.getColumnTypeName(i+1).equalsIgnoreCase("NUMBER")
					&& !meta2.getColumnTypeName(i+1).startsWith("INT")
					&& !meta2.getColumnTypeName(i+1).startsWith("MEDIUMINT")
					&& !meta2.getColumnTypeName(i+1).startsWith("TINYINT")
					&& !meta2.getColumnTypeName(i+1).startsWith("BIGINT")
					&& !meta2.getColumnTypeName(i+1).startsWith("DECIMAL")){
				sql.append("string, ");
				continue;
			}
			if (meta2.getColumnTypeName(i+1).startsWith("TINYINT")) {
				sql.append("TINYINT, ");
				continue;
			}
			if(meta2.getScale(i+1)>0){
				sql.append("double, ");
				continue;
			}		    	
			if( meta2.getPrecision(i+1)>9){
				sql.append("bigint, ");
				continue;
			}
			sql.append("int, ");  	
		}		    
		conn.close();
		return sql.substring(0, sql.length()-2); 
	}

	public static String getColumns(String table) throws Exception{
		Connection conn = DBConnection.getInstance(null) ;
		Statement stmt = conn.createStatement();	    

		ResultSetMetaData meta2 = stmt.executeQuery("select * from "+ table +" where 1=0").getMetaData();
		StringBuilder sql=new StringBuilder();
		for(int i=0;i<meta2.getColumnCount();i++){
			sql.append(meta2.getColumnName(i+1)+", ");	
		}		    
		conn.close();
		return sql.substring(0, sql.length()-2); 
	}



	public static void printCreateStatement(String table, String destTable,int isExternal) throws Exception{
		String cols = getSchema(table);
		String createSql = "CREATE ";
		if (isExternal==0) {
			createSql += "EXTERNAL " ;
		}
		createSql += " TABLE "+ destTable + " ("+ cols + ")" ;
		if (isExternal==0) {
//			createSql += " PARTITIONED BY(ds STRING) LOCATION '/data/share/"+destTable+"'" ;
			createSql += " PARTITIONED BY(ds STRING) " ;
		}
		System.out.println(createSql+";");		
	}	

	public static void printColumns(String table, String db) throws Exception{
		String cols = getColumns(table);
		System.out.println("\""+ cols + "\"");		
	}

	public static void createOptionsFile(String table, String db, String destTable, String path) throws Exception{

		File file = new File(path + table +".opt");
		if(file.exists()){
			file.delete();
		}
		file.createNewFile();

		PrintWriter pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream(file)));
		pw.println("import");
		pw.println("--connect");


		String url = "jdbc:oracle:thin:@10.0.0.52:1522/edw1";
		String user="qry";
		String prefix = "edw1_user";
		if("55".equalsIgnoreCase(db)){
			url = "jdbc:oracle:thin:@10.0.0.55:1522/track1";
			user = "track1_user";
			prefix = user;		
		}
		pw.println(url);
		pw.println("--username");	
		pw.println(user);
		pw.println("--password");
		pw.println("yihaodian");				
		pw.println("--m");
		pw.println("1");

		pw.println("--table");
		pw.println(prefix + "." + table );	
		pw.println("--columns");
		pw.println("\""+ getColumns(prefix + "." + table) +"\"" );
		pw.println("--hive-import");				
		pw.println("--hive-table");
		pw.println("zyl_"+ table);		
		pw.println("--hive-drop-import-delims" );
		pw.flush();
		pw.close();		
	}	
	
	public static void initDBDriver(String type) throws Exception{
		if("oracle".equalsIgnoreCase(type)){
			OracleDriver driver=new oracle.jdbc.OracleDriver();
			java.sql.DriverManager.registerDriver(driver);
		}else{
			com.mysql.jdbc.Driver driver2=new com.mysql.jdbc.Driver();
			java.sql.DriverManager.registerDriver(driver2);
		}
	}
	
	
	

/*	public static void main(String[] ar)throws Exception{
		String[] temp = {"exec", "E:\\lib\\testSession.properties"};
		main1(temp);
	}
	*/	
	
	public static void main(String[] argc)throws Exception
	{
		//测试参数
//		String[] argc2 = {"dump", "c:/test/url_referer.properties"};
//		argc = argc2;
		
		int len = argc.length;
		if(len<1){
			System.out.println("Error, the table name should be set. value is (build, createStatement)."); 
			return;
		}
		String[] commands={"buildOption", "createStatement", "dump", "query", "encrypt", "batchDump", "exec"};	

		int index=0;
		String param1= argc[index];		 
		if(param1.equals("-help")){
			System.out.println(" The valid value should be in " +  java.util.Arrays.toString(commands)+"."); 
			return; 
		}
		if(param1.equals("batchDump")){
		    BatchDump.scanTask(); 
			return; 
		}		

		ParseCommand pc = new ParseCommand(argc);
		
		String command=param1;
		index++;
		
		if(command.equals("encrypt")){
			String degree = pc.getParam("degree", "1");
			String p = pc.getParam("P");
			System.out.println("Encrypted Password:"+ EncrypterFactory.getEncryperInstance(degree).encode(p));	
			return;
		}			
//		// dump 异步回写
//		if(command.equals("dump")){
//			if(argc.length<=index){
//				log.fatal("Faild to dump data without config file settting.");
//				System.exit(Constants.STATUS_FAILED);
//			}
//		    BatchDump.commitTask(argc[index], pc.getParamMap());
//			System.exit(Constants.STATUS_SUCCESS); 
//		}		
		
		String destType=pc.getParam("type", "oracle");
		initDBDriver(destType);
		
		if(param1.equals("exec")){	
			log.info("Start to dump data ....");
			DumpProxy dp = new DumpProxy();
			boolean load = false;
			if(argc.length>index){
				load = dp.init(argc[index]);
			}else{
				load = dp.init(null);
			}	
			dp.setExternalParameter(pc.getParamMap());
			if(!load){
				log.error("Failed to load the basic configuration, Dump Proxy will exit directly.");
				System.exit(Constants.STATUS_FAILED);
			}		 
			
			try{
				dp.exportHive2DB();	
				System.exit(Constants.STATUS_SUCCESS);
			}catch(Exception ex){
				log.error("Task Failed: ", ex);
				System.exit(Constants.STATUS_FAILED);
			}
		}
		if ("etl".equalsIgnoreCase(param1)) {
//			HDFSExtract extract = new HDFSExtract();
//			extract.extract(argc);
			EtlMain.main(argc);
			return;
		}
		if ("archive".equalsIgnoreCase(param1)) {
			ArchiveOraToHive archive = new ArchiveOraToHive();
			archive.extract(argc) ;
			return;
		}

		if(param1.equals("query")){	
			log.info("Load data from oracle db ....");
			if(argc.length<=index+1 || (!argc[index].startsWith("-"))){
				log.error("You should specify the sql file with -f or sql with -q.");
				System.exit(0);			 
			}
			String method= argc[index].trim();
			String query= null;
			if("-f".equals(method)){
				DumpProxy dp = new DumpProxy();
				boolean load = dp.init(argc[index+1].trim());
				if(!load){
					log.error("Failed to load the basic configuration, Dump Proxy will exit directly.");
					System.exit(0);
				}	
				Connection conn = dp.getOracleConnection();
				Utils.batchExecute(conn, dp.getListProperties("preprocess.sql"), "SQL");
				query = dp.getStringProperties("query.sql").trim();
				if(query.length()>0){
					Statement stat = conn.createStatement();
					ResultSet rs = stat.executeQuery(Utils.parseCommand(query));
					int cols = rs.getMetaData().getColumnCount();
					String[] val = new String[cols];
					for(int i=0; i<cols; i++){
						val[i]= rs.getMetaData().getColumnName(i+1);
					}
					String out = java.util.Arrays.toString(val);
					out=out.substring(1, out.length()-1);
					out = out.replaceAll(",", "\t");						
					System.out.println(out);
					System.out.println("-------------------------------------------------------------------------------------");

					while(rs.next()){					
						for(int i=0; i<cols; i++){
							val[i]= rs.getString(i+1);
						}
						out = java.util.Arrays.toString(val);
						out=out.substring(1, out.length()-1);
						out = out.replaceAll(",", "\t");						
						System.out.println(out);
					}
					rs.close();               
				}
				Utils.batchExecute(conn, dp.getListProperties("sufprocess.sql"), "SQL");
				conn.close();
				return;
			}				
		}

		String db=pc.getParam("db", "52");
		String tableName=pc.getParam("table", "");
		String destTable=pc.getParam("destTable","");
		String destPath=pc.getParam("path", "/opt/project/Refresher/options/");	


		if(tableName.length()==0){
			log.error("The table name should be specified. "); 
			return;	 
		}
		if(destTable.length()==0){
			log.warn("No destination table was specified, default value was used.");
			destTable= "zyl_"+tableName;
		}
		
		if(command.equals("buildOption")){
			log.info("Start to build option file ...");
			createOptionsFile(tableName, db, destTable, destPath);
			log.info("Build option file successfully into path "+destPath +".");			 
		}else if(command.equals("createStatement")){	
			log.info("Start to print create statement ...");
			printCreateStatement(tableName, destTable,0);
			log.info("Create Statement Finished");			 
		}else{
			log.error(" The valid value should be in " +  java.util.Arrays.toString(commands)+".");	 			 
		}

	}

}
