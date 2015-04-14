package com.juanpi.hive.udf;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

public class IsAttack extends UDAF {
   /*public static class AvgState {
     private long mCount;
     private double mSum;
   }*/
   //List<Long> timestampList = new ArrayList<Long>();

   public static class AvgEvaluator implements UDAFEvaluator {
	   
     //AvgState state;
     List<Long> timestampList ;
     private static int maxpv = 8600;
     private static int seconds = 5*1000;
     private static int num_of_times = 20;
     
     public AvgEvaluator() {
        super();
        //state = new AvgState();
        timestampList = new ArrayList<Long>();
        init();
     }

     /**
      * init函数类似于构造函数，用于UDAF的初始化
      */
     public void init() {
        //state.mSum = 0;
        //state.mCount = 0;
    	 timestampList.clear();
     }

     /**
      * iterate接收传入的参数，并进行内部的轮转。其返回类型为boolean
      * 
      * @param o
      * @return
      */
     public boolean iterate(Long o) {
        if (o != null) {
          //state.mSum += o;
          //state.mCount++;
          timestampList.add(o);
        }
        return true;
     }

     /**
      * terminatePartial无参数，其为iterate函数轮转结束后，返回轮转数据，
      * terminatePartial类似于hadoop的Combiner
      * 
      * @return
      */
     public List<Long> terminatePartial() {// combiner
        return timestampList.size() == 0 ? null : timestampList;
     }

     /**
      * merge接收terminatePartial的返回结果，进行数据merge操作，其返回类型为boolean
      * 
      * @param o
      * @return
      */
     public boolean merge(List<Long> o) {
        if (o != null) {
        	//if ( o.size() > 0 ) {
        		/*state.mCount += o.mCount;
                state.mSum += o.mSum;*/
        		timestampList.addAll(o);
        	//}
        }
        return true;
     }

     /**
      * terminate返回最终的聚集函数结果
      * 
      * @return
      */
     public int terminate() {
    	 
    	 if ( timestampList.size() > maxpv ) {
    		 return timestampList.size();
    	 } 
    	 
    	 int loop = 0;
    	 //for loop
    	 for (Long timestamp :timestampList) {
    		 
    		 if ( loop >= num_of_times) {
    			 
    			 if ( timestamp == null ) {
    				 continue;
    			 } 
    			 
    			 if ( seconds >= Math.abs(timestamp - timestampList.get( loop - num_of_times) ) ) {
    				 return -1;
    			 }
    		 }
    		 
    		 loop++;
    		 
    	 }
    	 
    	 return 0;

     }
   }
}
