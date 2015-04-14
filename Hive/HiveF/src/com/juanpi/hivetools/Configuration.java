package com.juanpi.hivetools;

import java.io.File;

public class Configuration {
	
	
	public static String HIVE_CONFIG_HOME=null;	
	
	private static String initString="";	

	public static boolean loadInitString() throws HivePathException{
		HIVE_CONFIG_HOME=System.getenv("HIVE_CONFIG_HOME");
		if(HIVE_CONFIG_HOME==null||HIVE_CONFIG_HOME.length()==0){
			throw new HivePathException("Error: Failed to find the HIVE_CONFIG_HOME.");
		}
		File home=new File(HIVE_CONFIG_HOME);
		if(!home.exists()){
			throw new HivePathException("Error: The HIVE_HOME "+HIVE_CONFIG_HOME+" is not exist.");
		}		
		File addUDF=new File(home, "init.hql");
		
		if(!addUDF.exists()){
			throw new HivePathException("Error: Failed to find script "+ addUDF.getAbsolutePath() +", pls check the existence of this file.");
		}
		try{
			initString=Utils.getCommandFromFile(addUDF);
		}catch(Exception ex){		
			throw new HivePathException("Error: Failed to load script from + "+ addUDF.getAbsolutePath()+", detail:"+ ex.getMessage());
		}		
		return true;		
	}
	
	
	public static String getInitString(){
		return initString;
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	

}
