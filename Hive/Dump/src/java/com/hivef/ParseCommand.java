package com.hivef;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ParseCommand {
	
	private Map<String, String> map = null;
	private List<String> command = null;
	
	public ParseCommand(String[] argc){
		map =new HashMap<String, String>();
		command = new ArrayList<String>();
		if(argc.length==0){
			return;
		}
			
		int index = 0;
		
		while(index<=argc.length-1){
			String par =  argc[index].trim();
			if(par.startsWith("-")){
				String key = par.substring(1).trim();
				index++;
				String value=null;
				if(argc.length>index){
					value = argc[index].trim();
					if(value.startsWith("\"")|| value.startsWith("\'")){
						value =value.substring(1, value.length()-1).trim();
					}
				}
				map.put(key, value);
				index++;
			}else {
				command.add(par);
				index++;
			}			
		}
	}
	
	
	public Map<String, String> getParamMap(){
		return map;
	}
	
	public List<String> getParamList(){
		return command;
	}
	
	public String getParam(String name){
		return map.get(name);
	}   
	
	public String getParam(String name, String defaultValue){
		return map.get(name)==null ? defaultValue: map.get(name);
	}
    	
	public boolean useCommand(String cmd){
		return command.contains(cmd);
	}   
  
	
	public static void main(String[] argc){
		String[] test = {"build", "-db", "52", "-test", "tt"};
		
		ParseCommand command = new ParseCommand(test);
		System.out.println(command.getParam("db"));
		
		System.out.println(command.useCommand("build"));		
		System.out.println(command.useCommand("52"));		
		
		
	}

}
