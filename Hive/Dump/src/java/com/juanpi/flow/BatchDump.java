package com.juanpi.flow;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class BatchDump {
	
	private static Log log= LogFactory.getLog(BatchDump.class);	
	
	public static final int STATUS_IN_PROGRESS = 1;
	public static final int STATUS_FAILED      = 2;
	public static final int STATUS_FINISHED    = 3;
	
	public static final long MAX_EXECUTE_TIME = 600000; 
	
	public static final String lockFile = "SecureFlow.lock";
	
	private static String lockFilePath="";
	
	private static Connection centralConn = null;
	
	private static List<DumpTask> tasks = new ArrayList<DumpTask>();
	
	private static String  FLOW_HOME;
	
	
	static {
		FLOW_HOME=System.getenv("FLOW_HOME");
		if(FLOW_HOME==null&&FLOW_HOME.trim().length()==0){
			log.fatal("The FLOW_HOME is not set, System exit directly.");
			System.exit(1);
		}
		lockFilePath= FLOW_HOME+"/runtime/" + lockFile;		
	}	
	
	public static boolean commitTask(String filePath, Map<String, String> paramMap){
		System.setProperty("java.security.egd", "file:///dev/urandom");
		try{
			String paramString = paramMap.toString();			
//			loadCentralizeDBConnection();
			Statement stat =centralConn.createStatement();
			File file = new File(filePath);			
			stat.executeUpdate(ConnManager.getCommitTaskSql(file.getAbsolutePath(), paramString));
			centralConn.commit();
			centralConn.close();
			log.info("Success to commit the tasks");
			return true;
			
		}catch(Exception ex){
			log.error("Failed to commit the task.", ex);
			return false;
		}		
	}
	
	
	
	public static class DumpTask{
		
		public long taskId=0l;
		public String filePath="";
		public Map<String, String> params =new HashMap<String,String>();
		
		public void parseParamFromString(String paramMap){
			params.clear();
			if(paramMap==null||paramMap.trim().length()==0){
				return;
			}			
			paramMap=paramMap.trim();
			if(paramMap.startsWith("{")){
				paramMap=paramMap.substring(1);
			}
			if(paramMap.endsWith("}")){
				paramMap=paramMap.substring(0, paramMap.length()-1);
			}
			if(paramMap.trim().length()==0){
				return;
			}
			String[] pairs = paramMap.trim().split(",");
			for(String pair:pairs){
			   String[] kv= pair.trim().split("=");
			   if(kv.length>1){
				   params.put(kv[0], kv[1]);
			   }			   				
			}			
		}		
	}	
	
	private static void loadTask() throws Exception{
		tasks.clear();
		System.setProperty("java.security.egd", "file:///dev/urandom");
//		loadCentralizeDBConnection();
		Statement q =centralConn.createStatement();
		
		ResultSet rs = q.executeQuery(ConnManager.getBatchTaskQuerySql());
		while (rs.next()){
			DumpTask dt =new DumpTask();
			dt.taskId=rs.getLong(1);
			dt.filePath=rs.getString(2);
			dt.parseParamFromString(rs.getString(3));	
			tasks.add(dt);
		}
		rs.close();		
	}
	
	
	private static void execute(){
		
		for(DumpTask task: tasks){
			if(!needExecute(task.taskId)){
				break;
			}			
			log.info("Start to exec dump task "+ task.taskId +" ....");
				
			updateTaskStatus(task.taskId, STATUS_IN_PROGRESS, "");
			DumpProxy dp = new DumpProxy();
			boolean load = dp.init(task.filePath);
			dp.setExternalParameter(task.params);
			String error ="";
			if(!load){
				error="Failed to load the basic configuration, Dump Proxy will exit directly.";
				log.fatal(error);
				updateTaskStatus(task.taskId, STATUS_FAILED, error);
				break;
			}
			
			try{
				dp.exportHive2DB();
				updateTaskStatus(task.taskId, STATUS_FINISHED, "");
			}catch(Exception ex){
				log.error("Failed to execute the task "+ task.taskId+".");
				updateTaskStatus(task.taskId, STATUS_FAILED, StringEscape.validate(ex.getMessage(), 1000));
			}
			
			try{
				if(centralConn!=null){
					centralConn.commit();
				}				
			}catch(Exception ex){
				//ignore this exception;
			}	
			log.info("Finish to exec dump task "+ task.taskId +"");
		}	
		try{
			if(centralConn!=null){
				centralConn.close();
			}				
		}catch(Exception ex){
			//ignore this exception;
		}	
	}
	
	public static void scanTask(){
		if(isLocked()){
			log.info("The SecureFlow is running, exit directly ........");
		}else{
			lock();
			try{
				loadTask();
				execute();
			}catch(Exception ex){
				log.error("Failed to load Task ......", ex);
			}			
			unlock();
		}		
	}
	
	
	

	private static void updateTaskStatus(Long taskID, int status, String message){
		try{
			Statement stat= centralConn.createStatement();
			String updateSql = ConnManager.getUpdateTaskSql(taskID, status, StringEscape.escapingSqlString(message));
			stat.executeUpdate(updateSql);
			centralConn.commit();
		}catch(Exception ex){
			log.error("Failed to update status of " + taskID +" to "+ status +". ", ex);
		}
	}
	
	private static boolean needExecute(Long taskID){
		boolean needExecute=false;
		try{
			Statement stat= centralConn.createStatement();
			String updateSql =ConnManager.getCheckStatusSql(taskID);		
			ResultSet rs =stat.executeQuery(updateSql);
		    if(rs.next()){
		    	if(rs.getInt(1)==0){
		    		needExecute=true;
		    	}
		    }
		    rs.close();
		    stat.close();

		}catch(Exception ex){
			log.warn("Failed to get status of " + taskID +", ignore it directly.", ex);
		}
		return needExecute;
	}
	
	
//	private static void loadCentralizeDBConnection() throws Exception{		
//		ConnManager.ConnectionConfig config = ConnManager.getCenterizeDBConfig();
//		Class.forName("oracle.jdbc.driver.OracleDriver");
//		centralConn = DriverManager.getConnection(config.connURL, config.connUser, 
//			 EncrypterFactory.getEncryperInstance(1).decode(config.encryptedPassword));
//	}
	
	
	private static void lock(){
		File file = new File(lockFilePath);
		if(!file.exists()){
			try{
				file.createNewFile();
			}catch(Exception ex){
				//ignore the lock error;
			}			
		}		
	}
	
	private static void unlock(){
		File file = new File(lockFilePath);
		if(file.exists()){
			try{
				file.delete();
			}catch(Exception ex){
				//ignore the lock error;
			}			
		}		
	}
	
	
	public static boolean isLocked(){
		File file = new File(lockFilePath);
		long stayTime= System.currentTimeMillis() - file.lastModified();
		if(stayTime>MAX_EXECUTE_TIME){
			file.delete();
			return false;
		}
		return file.exists();		
	}
	
	
	
	public static void main(String[] argc){
		Map<String, String> map = new HashMap<String, String>();
		map.put("k1", "v1");
		map.put("k2", "v2");
		System.out.println(map);
		
		DumpTask dt = new DumpTask();
		dt.parseParamFromString(map.toString());
		System.out.println(dt.params);
		
		System.out.println(System.currentTimeMillis()-new File("E:\\lib\\SecureFlow_zyl.jar").lastModified());
		
		System.out.println(commitTask("E:\\lib\\SecureFlow_zyl.jar", map));
		
	}
	
	
	
	
	
	

}
