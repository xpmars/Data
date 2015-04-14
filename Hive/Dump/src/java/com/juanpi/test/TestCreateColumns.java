package com.juanpi.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;


public class TestCreateColumns {

	
	public static void main(String[] args) throws Exception {
		File file = new File("c:/columns.txt");
		FileReader reader = new FileReader(file);
		BufferedReader br = new BufferedReader(reader);
		String aline = null;
		String arr[] = null;
		StringBuffer sBuffer = new StringBuffer();
		while((aline=br.readLine())!=null)
		{
			if(!"".equals(aline) && !aline.startsWith("#"))
			{
				arr = aline.trim().split(" ");
				sBuffer.append(arr[0]+",");
			}
		
		}
		System.out.println(sBuffer.toString().substring(0, sBuffer.toString().length()-1));
		br.close();
		reader.close();
	}
}
