package com.juanpi.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.io.InputStreamReader;


public class DealSourceTrack {

	/**该类对track原始数据作如下动作：
	 * 1、格式转换，分隔符改为\t
	 * 2、行首增加自增ID
	 * 最后生成一个新文件
	 * @param 参数1是输入文件名，参数2是输出文件名
	 */
	public static String ID_CONF = null ;
	public static void main(String[] args) {
		if (args.length != 3) {
			System.err.println("args is not enough!");
			return ;
		}
		try {
			ID_CONF = args[2];
			File inputFile = new File(args[0]) ;
//			FileReader reader = new FileReader(inputFile);
//			BufferedReader br = new BufferedReader(reader);
			InputStream  is = new FileInputStream(inputFile);
			BufferedReader br = new BufferedReader(new InputStreamReader(is,"UTF-8"));
			StringBuffer sBuffer = new StringBuffer();
			String aline = null;
			long id=getID(ID_CONF);
			while((aline=br.readLine())!=null)
			{
				id ++;
				aline = aline.replaceAll("\",\"", "\t");
				aline = aline.replaceAll("\"", "");
				sBuffer.append(id+"\t"+aline+"\n") ;
			}
			//更新ID配置文件
			updateIDConf(ID_CONF,id+"");
			//生成新文件
			File outFile = new File(args[1]);
			if(! outFile.exists())
			{
				outFile.createNewFile();
			}
//			byte[] b = sBuffer.toString().getBytes();
			byte[] b = new String(sBuffer.toString().getBytes(),"UTF-8").getBytes();
			FileOutputStream fs = new FileOutputStream(outFile) ;
			fs.write(b);
			fs.close();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	public static long getID(String fileName) throws Exception{
		long id=0;
		File file = new File(fileName);
		FileReader reader = new FileReader(file);
		BufferedReader br = new BufferedReader(reader);
		String aline = null;
		while((aline=br.readLine())!=null)
		{
			if(!"".equals(aline) && !aline.startsWith("#") && !aline.startsWith("--"))
			{
				try {
					id = Long.parseLong(aline.trim());
				} catch (Exception e) {
					e.printStackTrace();
				}
				break;
			}
		}
		br.close();
		reader.close();
		return id;
	}

	public static void updateIDConf(String fileName,String id)
	{
		try {
			File conf = new File(fileName);
			if(! conf.exists())
			{
				conf.createNewFile();
			}
			byte[] b2 = id.getBytes();
			FileOutputStream fs2 = new FileOutputStream(conf) ;
			fs2.write(b2);
			fs2.close();
		} catch (Exception e) {
			e.printStackTrace() ;
		}
	}
}
