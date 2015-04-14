package com.juanpi.flow;
/**
 * 
 * @author lvpeng
 * 该类是发etl预警邮件所用
 *
 */
public class ETL_vo {

	private String tableName;
	private String beginTime;
	private String endTime;
	private String result;
	private String bkDate ;
	public String getBkDate() {
		return bkDate;
	}
	public void setBkDate(String bkDate) {
		this.bkDate = bkDate;
	}
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	public String getEndTime() {
		return endTime;
	}
	public void setEndTime(String endTime) {
		this.endTime = endTime;
	}
	public String getResult() {
		return result;
	}
	public void setResult(String result) {
		this.result = result;
	}
	public String getBeginTime() {
		return beginTime;
	}
	public void setBeginTime(String beginTime) {
		this.beginTime = beginTime;
	}
	
}
