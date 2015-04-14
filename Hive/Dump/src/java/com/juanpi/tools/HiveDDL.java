package com.juanpi.tools;


import java.sql.SQLException;

import com.juanpi.flow.Flow;


public class HiveDDL {

	/**
	 * @param args
	 * @throws SQLException 
	 * @throws Exception 
	 */
	public static void main(String[] args) throws SQLException, Exception {
	 
		
		if(args.length<3)
		{
            System.out.println("use COMMAND [Args]");
            System.out.println("Args[0]: source_tableName");
            System.out.println("Args[1]: hive_tableName");
            System.out.println("Args[2]: 0 or 1, common table is 0, partition table is 1");
            
			throw new RuntimeException("args number must 3") ;
		}
//		args = new String[4];
//		args[0]="xd_js_goods_apply";
//		args[1]="xd_js_goods_apply";
//		args[2]="1";
		Flow.printCreateStatement(args[0],
							args[1].toLowerCase(),
							Integer.parseInt(args[2]) );

		
		
	}

		

}
