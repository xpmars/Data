package com.juanpi.flow;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class ConnLoader {
	
	public static final Log log = LogFactory.getLog(ConnLoader.class);
	
	
	private static String FLOW_HOME=null;
	
//	private static String connectionPath=null;
	
	static{
		FLOW_HOME = System.getenv("FLOW_HOME");
		if(FLOW_HOME==null || FLOW_HOME.length()==0){
			log.fatal("Failed to find FLOW_HOME.");
			FLOW_HOME = "E:" ;
//			System.exit(Constants.STATUS_INITIAL_FAILED);
		}
//		log.fatal("FLOW_HOME is  "+FLOW_HOME);
		File home= new File(FLOW_HOME);
		if(!home.exists()){
			log.fatal("The FLOW_HOME "+FLOW_HOME+ " does not exist.");
			System.exit(Constants.STATUS_INITIAL_FAILED);
		}		
//		File conns=new File(home, "connections");
//		if(!conns.exists()){
//			conns.mkdir();
//		}
//		connectionPath= conns.getAbsolutePath();
//		log.info("conf:"+connectionPath);
	}
	
	
	public static String getFlowHome(){
		return FLOW_HOME;
	}
//	
//	public static String getConnectionPath(){
//		return connectionPath;
//	}
	
	
	public static Properties getConnection() throws Exception{
		Properties prop =new Properties();
		prop.clear();
		InputStream stream = new FileInputStream(new File(getETLDBConfigFile()));
		prop.load(stream);
		stream.close();
		return prop;		
	}
	
	public static String getSqoopOptDir(){
		return FLOW_HOME+"/sqoop_opt"; 
	}
	
	public static String getETLDBConfigFile(){
		return FLOW_HOME+"/dump-default.properties"; 
	}
	public static String getETLDB(){
		return FLOW_HOME+"/connections/default.properties"; 
	}
	
	
	
	
	

}
