package com.juanpi.hive.udf;
import java.util.ArrayList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.juanpi.hive.utils.UDFConstant;
/**
 * 分组函数，按索引查询组内某字段col1第n个值对应的col2的值, 如索引值为负值，则从上到下取，否则从下到上，索引从1开始 ， 不忽略目标空值
 * 如-1 为取第一个
 *   1 为取最后一个
 * @author zhuokun
 *
 */

@Description(name = "FindRecord", value = "_FUNC_(expr1, int, expr2) - 分组函数，按索引查询组内某字段col1第n个值对应的col2的值, 如索引值为负，则从小到大取，否则从大到小，索引从1开始 ， 不忽略目标空值.")
public class FindRecord extends UDAF {
	
	public static Log log = LogFactory.getLog(FindRecord.class); 
	
	public static class BaseEvaluator implements UDAFEvaluator{

		protected String partial = new String();

		public void init() {
			partial = new String();	
		}

		public boolean baseIterate(String sortable, Integer param, String value) throws JSONException{
			if (sortable == null || "null".equalsIgnoreCase(sortable) || param==0){
				return true;
			}
			
			JSONObject obj = null;
			if (partial.length() == 0){
				obj = new JSONObject();
				obj.put("index", Math.abs(param));
				obj.put("min", param<0);		
			}else{
				obj = new JSONObject(partial);				
			}
			
			obj.append("labels", value);
			obj.append("values", sortable);	
			partial = obj.toString();
			return true;
		}

		public String terminatePartial(){
			return partial;
		}

		public boolean merge(String other) throws JSONException{
			if (other == null || other.length()==0){
				return true;
			}			
			if (partial.length() == 0){			
				partial = new String(other);
				return true;						
			}
			
			JSONObject obj = new JSONObject(partial);				
			JSONObject o = new JSONObject(other);			
			JSONArray addLabel = o.getJSONArray("labels");			
			JSONArray addValue = o.getJSONArray("values");
			
			for(int i=0;i<addLabel.length(); i++){
				obj.append("labels", addLabel.getString(i));
				obj.append("values", addValue.get(i));				
			}			
			partial = obj.toString();		
			return true;
		}
    }
	

	public static class FindStringWithIntegerUDAFEvaluator extends BaseEvaluator implements UDAFEvaluator{
		
		public boolean iterate(Integer sortable, Integer param, String label)throws JSONException{
			if(sortable ==null){
				return true;
			}
			if(label==null){
				label = UDFConstant.NULL_ESCAPE;
			}
			return baseIterate(sortable.toString(), param, label);
		}		
		
		public String terminate() throws JSONException{
			if(partial.length()==0){
				return null;				
			}			
			JSONObject obj = new JSONObject(partial);			
			JSONArray labels = obj.getJSONArray("labels");			
			JSONArray values = obj.getJSONArray("values");
			if(labels.length()==0){
				return null;
			}
			int index = obj.getInt("index");
			boolean min = obj.getBoolean("min");
			
			if(index>labels.length()){
				return null;
			}			
			Map<Integer, String> map = new HashMap<Integer, String>();	
			List<Integer> keys = new ArrayList<Integer>();
			for(int i=0; i<labels.length(); i++){
				map.put(values.getInt(i), labels.getString(i));
				keys.add(values.getInt(i));
			}
			java.util.Collections.sort(keys);		
			
			int ind = min? (index-1):(keys.size()-index);
			String res = map.get(keys.get(ind));
			return UDFConstant.NULL_ESCAPE.equals(res)? null:res;		
		}
		
	}
	
	
	
	public static class FindStringWithLongUDAFEvaluator extends BaseEvaluator implements UDAFEvaluator{
		
		public boolean iterate(Long sortable, Integer param, String label)throws JSONException{
			if(sortable ==null){
				return true;
			}
			if(label==null){
				label = UDFConstant.NULL_ESCAPE;
			}
			return baseIterate(sortable.toString(), param, label);
		}		
		
		public String terminate() throws JSONException{
			if(partial.length()==0){
				return null;				
			}			
			JSONObject obj = new JSONObject(partial);			
			JSONArray labels = obj.getJSONArray("labels");			
			JSONArray values = obj.getJSONArray("values");
			if(labels.length()==0){
				return null;
			}
			int index = obj.getInt("index");
			boolean min = obj.getBoolean("min");			
			
			if(index>labels.length()){
				return null;
			}			
			Map<Long, String> map = new HashMap<Long, String>();	
			List<Long> keys = new ArrayList<Long>();
			for(int i=0; i<labels.length(); i++){
				map.put(values.getLong(i), labels.getString(i));
				keys.add(values.getLong(i));
			}
			java.util.Collections.sort(keys);
			
			int ind = min? (index-1):(keys.size()-index);
			String res = map.get(keys.get(ind));
			return UDFConstant.NULL_ESCAPE.equals(res)? null:res;		
		}	
					
	}
	
	
	public static class FindStringWithDoubleUDAFEvaluator extends BaseEvaluator implements UDAFEvaluator{
		
		public boolean iterate(Double sortable, Integer param, String label)throws JSONException{
			if(sortable ==null){
				return true;
			}
			if(label==null){
				label = UDFConstant.NULL_ESCAPE;
			}
			return baseIterate(sortable.toString(), param, label);
		}		
		
		public String terminate() throws JSONException{
			if(partial.length()==0){
				return null;				
			}			
			JSONObject obj = new JSONObject(partial);			
			JSONArray labels = obj.getJSONArray("labels");			
			JSONArray values = obj.getJSONArray("values");
			if(labels.length()==0){
				return null;
			}
			int index = obj.getInt("index");
			boolean min = obj.getBoolean("min");
				
			if(index>labels.length()){
				return null;
			}			
			Map<Double, String> map = new HashMap<Double, String>();	
			List<Double> keys = new ArrayList<Double>();
			for(int i=0; i<labels.length(); i++){
				map.put(values.getDouble(i), labels.getString(i));
				keys.add(values.getDouble(i));
			}
			java.util.Collections.sort(keys);
			int ind = min? (index-1):(keys.size()-index);
			String res = map.get(keys.get(ind));
			return UDFConstant.NULL_ESCAPE.equals(res)? null:res;	
		}		
	}	
	
	
	public static class FindStringWithStringUDAFEvaluator extends BaseEvaluator implements UDAFEvaluator{
		
		public boolean iterate(String sortable, Integer param, String label)throws JSONException{
			if(sortable ==null){
				return true;
			}
			if(label==null){
				label = UDFConstant.NULL_ESCAPE;
			}
			return baseIterate(sortable.toString(), param, label);
		}		
		
		public String terminate() throws JSONException{
			if(partial.length()==0){
				return null;				
			}			
			JSONObject obj = new JSONObject(partial);			
			JSONArray labels = obj.getJSONArray("labels");			
			JSONArray values = obj.getJSONArray("values");
			if(labels.length()==0){
				return null;
			}
			int index = obj.getInt("index");
			boolean min = obj.getBoolean("min");
				
			if(index>labels.length()){
				return null;
			}			
			Map<String, String> map = new HashMap<String, String>();	
			List<String> keys = new ArrayList<String>();
			for(int i=0; i<labels.length(); i++){
				map.put(values.getString(i), labels.getString(i));
				keys.add(values.getString(i));
			}
			java.util.Collections.sort(keys);
			int ind = min? (index-1):(keys.size()-index);
			String res = map.get(keys.get(ind));
			return UDFConstant.NULL_ESCAPE.equals(res)? null:res;	
		}		
	}	

	
	public static void main(String[] argc){
		List<String> test1 = new ArrayList<String>();
		List<Long> test2 = new ArrayList<Long>();
		test1.add("abc");
		test2.add(123l);

		test1.add("def");
		test2.add(32l);
		
		test1.add("pc");
		test2.add(23l);
		
		test1.add("c");
		test2.add(1355l);
		
		Map<String, Long> map = new HashMap<String, Long>();			
		for(int i=0; i< test1.size(); i++){
			map.put(test1.get(i), test2.get(i));
		}	

		
	}
	
	
	
	
	
}