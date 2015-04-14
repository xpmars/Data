package com.juanpi.flow;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.File;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class Configuration {
	
	private  Properties prop = new Properties();
	
	public static final Log log = LogFactory.getLog(Configuration.class);
	
	private String configFile= "";
	
	
	public boolean initialize(String configFilePath){
		this.configFile= configFilePath;
		log.info("configFilePath==="+configFilePath);
		try {
			prop.clear();
			InputStream stream = new FileInputStream(configFile);
			prop.load(stream);
			return true;
		} catch (Exception ex) {
			log.error("Failed to load the configuration, please check the file dataflow.properties", ex);
			return false;
		}
	}	
	
	public Properties getProperties(){
		return prop;
	}
}
