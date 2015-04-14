package com.juanpi.flow;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

public class DBConnection {
	private static Connection con = null;
	private DBConnection() {}
	
	public static Connection getInstance(String path) throws Exception
	{
		Properties prop = new Properties();
		InputStream stream = null;
		if (path == null || path.length() == 0) {
			stream = new FileInputStream(new File(ConnLoader.getETLDBConfigFile()));
		}else {
			stream = new FileInputStream(new File(path).getAbsoluteFile());
		}
		prop.load(stream);
		if (con == null) {
			Class.forName(prop.getProperty("db.driver"));
			con = DriverManager.getConnection(prop.getProperty("db.url"), prop.getProperty("db.user"), 
					EncrypterFactory.getEncryperInstance(1).decode(prop.getProperty("db.password"))
//					prop.getProperty("db.password")
					);
		}
		return con;
	}
	
	public static Connection getInstance() throws Exception
	{
		Properties prop = new Properties();
		InputStream stream = null;
		stream = new FileInputStream(new File(ConnLoader.getETLDBConfigFile()));
		prop.load(stream);
		if (con == null) {
			Class.forName(prop.getProperty("db.driver"));
			con = DriverManager.getConnection(prop.getProperty("db.url"), prop.getProperty("db.user"), 
					EncrypterFactory.getEncryperInstance(1).decode(prop.getProperty("db.password")));
		}
		return con;
	}
	
	public static Connection getEtlCon() throws Exception
	{
		Properties prop = new Properties();
		InputStream stream = null;
		stream = new FileInputStream(new File(ConnLoader.getETLDB()));
		prop.load(stream);
		if (con == null) {
			Class.forName(prop.getProperty("db.driver"));
			con = DriverManager.getConnection(prop.getProperty("db.url"), prop.getProperty("db.user"), 
					EncrypterFactory.getEncryperInstance(1).decode(prop.getProperty("db.password")));
		}
		return con;
	}
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		System.out.println(DBConnection.getInstance(null).toString());
	}

}
