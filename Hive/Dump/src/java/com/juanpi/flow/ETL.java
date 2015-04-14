package com.juanpi.flow;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;

public class ETL implements Runnable{

	/**
	 * @param args
	 * 该类负责管理HDFS的数据抽取工作
	 * useage:
	 * java -jar /opt/project/share/flow/SecureFlow.jar etl 即可
	 */
	ArrayList<ETLModel> list = null;
	public ETL(ArrayList<ETLModel> list )
	{
		this.list = list ;
	}
	
	public static Logger log = Logger.getLogger(ETL.class);
	private static String SQOOP_OPT_DIR = "" ; // sqoop option目录 c:/test   /opt/temp/tmp_sqoop_opt  
	
	SimpleDateFormat sfmt = new SimpleDateFormat("yyyy-MM-dd");
	
	File sqp_opt = null;
	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	@Override
	public void run() 
	{
		SQOOP_OPT_DIR= ConnLoader.getSqoopOptDir();		
		// 1 读取配置表内容 yhd_extract_to_hdfs
		try {
			
			Runtime run = null;
			List<ETL_vo> list = new ArrayList<ETL_vo>();
			for(ETLModel model : this.list)
			{
				ETL_vo vo = new ETL_vo();
				vo.setTableName(model.getDb_table());
				int flag = this.createOptionsFile(model);
				if (flag == -1) {
					log.info("Error: create option file fail, please check yhd_extract_to_hdfs table ! task_id 为"+model.getTask_id() +"    "+sdf.format(new Date())) ;
					continue ;
				}else {
					run = Runtime.getRuntime();
					try {
						if (model.getTarget_dir() != null && ! "".equals(model.getTarget_dir())) {
							run.exec("hadoop fs -rmr "+model.getTarget_dir());
						}else {
							run.exec("hadoop fs -rmr "+model.getDb_table().trim());
							run.exec("hadoop fs -rmr "+model.getDb_table().trim().toUpperCase());
							run.exec("hadoop fs -rmr "+model.getDb_table().trim().toLowerCase());
						}
						log.info("Begin excuete task "+model.getTask_id()+" "+model.getHive_table()+" ......"+sdf.format(new Date())) ;
						vo.setBeginTime(sdf.format(new Date()));
						vo.setResult("SUCCESS");
						Process p = run.exec("sqoop --options-file "+sqp_opt.getAbsolutePath());
						if (p.waitFor() != 0) {
							if (p.exitValue() == 1)//p.exitValue()==0表示正常结束，1：非正常结束  
							{
								System.err.println("Error: sqoop --options-file "+sqp_opt.getAbsolutePath() +" error! "+sdf.format(new Date())) ;
								log.info("Error: sqoop --options-file "+sqp_opt.getAbsolutePath() +" error! "+sdf.format(new Date())) ;
								vo.setEndTime(sdf.format(new Date()));
								vo.setResult("FAIL");
								log.info(readStream(p.getErrorStream()));
//								System.exit(1);
							}else {
								vo.setResult("SUCCESS");
							}
					    }
						vo.setEndTime(sdf.format(new Date()));
					} catch (Exception e) {
						log.info("Error: Job fail task_id is "+model.getTask_id() +"    "+sdf.format(new Date())) ;
						vo.setEndTime(sdf.format(new Date()));
						vo.setResult("FAIL");
						continue;
					}
				}
				list.add(vo);
			}
			log.info("Finish Extracting ! "+sdf.format(new Date())) ;
			
			saveResultReport(list);
		} catch (Exception e) {
			log.info("Error: extract fail !");
			e.printStackTrace();
			return ;
		}
	}
	public int createOptionsFile(ETLModel model) throws Exception
	{
		String where = null;
		int result = 0;
		sqp_opt = new File(SQOOP_OPT_DIR+"/tmp_"+model.getDb_table()+".opt");
		StringBuffer optSb = new StringBuffer();
		optSb.append("import \n");
//		optSb.append("-D mapreduce.job.queuename=bi_etl \n");
		optSb.append("--connect\n");
		optSb.append(model.getDb_url()+"\n");
		optSb.append("--username \n");
		optSb.append(model.getDb_userName()+"\n");
		optSb.append("--password \n");
		optSb.append(EncrypterFactory.getEncryperInstance(1).decode(model.getDb_passwd())+"\n");
		if (model.getMappers() != null ) 
		{  
			optSb.append("-m\n");
			optSb.append(model.getMappers()+"\n");
		}
		if (model.getSplit_by() != null && !"".equals(model.getSplit_by())) {
			optSb.append("--split-by\n");
			optSb.append(model.getSplit_by()+"\n");
		}
		
		optSb.append("--null-string\n");
		optSb.append("''\n");
		
		optSb.append("--table \n");
		optSb.append(model.getDb_table()+"\n");
		//以下是数据表非必填项，需要判断
		if(model.getDb_colunms() != null && ! "".equals(model.getDb_colunms())){
			optSb.append("--columns \n");
//			if (model.getIs_col_upper()==null || "Y".equalsIgnoreCase(model.getIs_col_upper())) {
//				optSb.append("\""+model.getDb_colunms().trim().toUpperCase()+"\"\n");
//			}
//			else {
				optSb.append("\""+model.getDb_colunms().trim()+"\"\n");
//			}
			
		}
		if (model.getWhere() != null && ! "".equals(model.getWhere())) {
			optSb.append("--where \n");
			where = model.getWhere().replaceAll("\\$TXDATE", "'"+model.getCount_date()+"'") ;
			optSb.append("\""+where.trim()+"\"\n");
		}
		
		if (model.getTarget_dir() != null && ! "".equals(model.getTarget_dir())) {
			optSb.append("--target-dir \n");
			optSb.append(model.getTarget_dir()+"\n");
		}
		
		if (model.getIs_hive_import() != null && ! "".equals(model.getIs_hive_import())
				&& "Y".equalsIgnoreCase(model.getIs_hive_import()))
		{
			optSb.append("--hive-overwrite \n");  // 统一为覆盖模式，要求hive 表必须存在
			optSb.append("--hive-import \n");
			optSb.append("--hive-drop-import-delims\n");
			
			if (model.getHive_table() == null || "".equals(model.getHive_table())) {
				log.info("Error: hive_table must be set 当--hive-import时 !");
				result = -1 ;
			}else {
				optSb.append("--hive-table \n");
				optSb.append(model.getHive_table().trim()+"\n");
			}
			if (model.getHive_pt_key() != null && !"".equals(model.getHive_pt_key())) {
				optSb.append("--hive-partition-key \n");
				optSb.append(model.getHive_pt_key().trim()+"\n");
				optSb.append("--hive-partition-value \n");
				optSb.append("\""+model.getCount_date()+"\"\n");
			}
		}
		
		if(! sqp_opt.exists())
		{
			try {
				sqp_opt.createNewFile();
			} catch (IOException e) {
				System.out.println("Create sqp_opt fail+++++++++++++++++++++++++");
			}
		}
		byte[] b = (optSb.toString()).getBytes();
		FileOutputStream fs = new FileOutputStream(sqp_opt);
		fs.write(b);
		fs.close();
		return result;
	}
	
	public void saveResultReport(List<ETL_vo> list) throws Exception
	{
		Connection con = DBConnection.getInstance();
		Statement st = con.createStatement();
		
		for (ETL_vo vo : list) {
			String sql = "insert into hadoop_etl_report values('"+vo.getTableName()+"','"+vo.getBeginTime()+"','"+vo.getEndTime()+"','"+vo.getResult()+"')";
			//log.info(sql);
			st.execute(sql);
		}
		st.close();
//		con.close() ;
	}
	
	public static String readStream(InputStream inStream) throws Exception {
		int count = inStream.available();
		byte[] b = new byte[count];
		inStream.read(b);
		return new String(b);
	}

	
}
