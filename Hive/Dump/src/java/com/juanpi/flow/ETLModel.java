package com.juanpi.flow;

public class ETLModel {
	private String count_date ;
	public String getCount_date() {
		return count_date;
	}
	public void setCount_date(String countDate) {
		count_date = countDate;
	}
	private int task_id;
	private String db_table;
	private String db_colunms;
	
	private String where;
	private String target_dir;
	private String is_hive_import;
	
	private String hive_table;
	private String hive_pt_key;
	private String is_use;
	private int priority;
	private Integer mappers;
	private String split_by;
	
	public String getIs_col_upper() {
		return is_col_upper;
	}
	public void setIs_col_upper(String isColUpper) {
		is_col_upper = isColUpper;
	}
	private String db_userName;
	private String db_passwd;
	private String db_url;
	private String is_col_upper;
	
	public int getTask_id() {
		return task_id;
	}
	public void setTask_id(int taskId) {
		task_id = taskId;
	}
	public String getDb_table() {
		return db_table;
	}
	public void setDb_table(String dbTable) {
		db_table = dbTable;
	}
	public String getDb_colunms() {
		return db_colunms;
	}
	public void setDb_colunms(String dbColunms) {
		db_colunms = dbColunms;
	}
	public String getWhere() {
		return where;
	}
	public void setWhere(String where) {
		this.where = where;
	}
	public String getTarget_dir() {
		return target_dir;
	}
	public void setTarget_dir(String targetDir) {
		target_dir = targetDir;
	}
	public String getIs_hive_import() {
		return is_hive_import;
	}
	public void setIs_hive_import(String isHiveImport) {
		is_hive_import = isHiveImport;
	}
	public String getHive_table() {
		return hive_table;
	}
	public void setHive_table(String hiveTable) {
		hive_table = hiveTable;
	}
	public String getHive_pt_key() {
		return hive_pt_key;
	}
	public void setHive_pt_key(String hivePtKey) {
		hive_pt_key = hivePtKey;
	}
	public String getIs_use() {
		return is_use;
	}
	public void setIs_use(String isUse) {
		is_use = isUse;
	}
	public int getPriority() {
		return priority;
	}
	public void setPriority(int priority) {
		this.priority = priority;
	}

	public Integer getMappers() {
		return mappers;
	}
	public void setMappers(Integer mappers) {
		this.mappers = mappers;
	}
	public String getSplit_by() {
		return split_by;
	}
	public void setSplit_by(String splitBy) {
		split_by = splitBy;
	}
	public String getDb_userName() {
		return db_userName;
	}
	public void setDb_userName(String dbUserName) {
		db_userName = dbUserName;
	}
	public String getDb_passwd() {
		return db_passwd;
	}
	public void setDb_passwd(String dbPasswd) {
		db_passwd = dbPasswd;
	}
	public String getDb_url() {
		return db_url;
	}
	public void setDb_url(String dbUrl) {
		db_url = dbUrl;
	}
	
	
	
	
}
