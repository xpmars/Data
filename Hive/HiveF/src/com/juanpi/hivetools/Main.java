package com.juanpi.hivetools;

import java.io.File;

public class Main {
	
	public static void main(String[] args) throws Exception{		
		int len = args.length;
		if(len<1){
			throw new Exception("Error: the hive sql file must be set!"); 
		}
		File file = new File(args[0]);
		if(!file.exists()){
			throw new Exception("Error: the hive sql file "+ args[0] +" does not exist!"); 
		}
		ParseCommand pc = new ParseCommand(args);
//		boolean isInit=false;		
//		String initString="";
	//	System.out.println(pc.getParamList());
//		if(pc.getParamList().contains("init")){
			//System.out.println("Init: true");
//			isInit=Configuration.loadInitString();
//			initString=Configuration.getInitString();
		//	System.out.println(initString);
//			isInit=true;
//		}
		String command=Utils.parseCommand(Utils.getCommandFromFile(file), pc.getParamMap());
//		if(isInit){
//			System.out.println(initString + Utils.filterInitString(command));
//		}else{
			System.out.println(command);
//		}				
	}
	
	
	
}
