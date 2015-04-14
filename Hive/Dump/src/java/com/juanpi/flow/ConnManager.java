package com.juanpi.flow;

public class ConnManager {
	
	
	public static enum ConnType{
		ORACLE,
		HIVE
	}
	
	public static class ConnectionConfig{
		public String connName="default";
        public ConnType connType=ConnType.ORACLE;
		public String connURL="";
		public String connUser="";
		public String encryptedPassword="";	
		
		public ConnectionConfig(String connName, ConnType connType, String connURL, String connUser, 
				 String connPassword){
			this.connName=connName;
			this.connType=connType;
			this.connURL=connURL;
			this.connUser=connUser;
			this.encryptedPassword=connPassword;	
		}				
	}

	
	public static String getBatchTaskQuerySql(){
		String localIP= Utils.getLocalIP();
		return "select id, config_file_path, param_map from rpt_flow_task where status=0 and ignore=0 and commit_time>=trunc(sysdate-1) and server_ip='"+
				localIP +"' order by commit_time";		
	}
	
	
	public static String getCommitTaskSql(String configFilePath, String paramString){		
		String localIP= Utils.getLocalIP();
		return "insert into rpt_flow_task (id, commit_time, config_file_path, param_map, status, ignore, server_ip) values " +
				"(seq_flow_id.nextval, sysdate,'" + configFilePath +"', '"+ paramString +"', 0, 0, '"+ localIP +"')";		
	}
	
	public static String getUpdateTaskSql(long taskID, int status, String message){
		return "update rpt_flow_task set status=" + status +", message='" + message+"' where id=" + taskID;
	}	
	
	
	public static String getCheckStatusSql(long taskID){
		return "select status from rpt_flow_task where id=" + taskID;
	}	
		
	public static void main(String[] arv){
	}

}
