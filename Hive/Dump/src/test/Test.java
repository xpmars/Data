import com.juanpi.flow.ResultSetAdapter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;


public class Test {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub

			String url="jdbc:hive2://192.168.2.4:10000/default";
			if(url.contains("jdbc:hive2")){
				org.apache.hive.jdbc.HiveDriver driver_new=new org.apache.hive.jdbc.HiveDriver();
				DriverManager.registerDriver(driver_new);
			}else{
				org.apache.hadoop.hive.jdbc.HiveDriver driver_old=
						new org.apache.hadoop.hive.jdbc.HiveDriver();
				DriverManager.registerDriver(driver_old);
			}
			Connection con = DriverManager.getConnection(url,"hadoop","123");
			System.out.println(con);
			String hqlString="select id from test";
			Statement statement = con.createStatement() ;
//			statement.execute("set mapred.job.queue.name=bi_etl");
//			ResultSetAdapter rs = new ResultSetAdapter(statement.executeQuery(hqlString));
			
			ResultSet rs = null;
			try {
				rs = statement.executeQuery(hqlString) ;
				System.out.println("success:"+hqlString);
			} catch (Exception e) {
				e.printStackTrace();
				System.out.println("fail:"+hqlString);
			}
//			hqlString="set mapred.job.queue.name=bi_etl;select session_id from dw.fct_session_info where ds='2014-07-01' limit 10";
//			try {
//				rs = statement.executeQuery(hqlString) ;
//				System.out.println("success:"+hqlString);
//			} catch (Exception e) {
//				System.out.println("fail:"+hqlString);
//			}
			
			
			int num=0;
//			while(rs.next())
//			{
//				num ++ ;
//			}
	
			System.out.println("rows:"+num);
			rs.close();
			statement.close();
			con.close();
	}

}
