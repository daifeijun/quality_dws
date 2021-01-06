package cn.com.microintelligence.sink;

import cn.com.microintelligence.common.InitProperties;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * 将计算的数据sink到监控表
 */
public class MonitorJdbcSink extends RichSinkFunction<Tuple3<Integer,String,String>> {
    Connection conn = null;
    PreparedStatement insertStmt = null;
    String url = InitProperties.bd_monitor_url;
    String username = InitProperties.mysql_username;
    String pw = InitProperties.mysql_pw;
    //PreparedStatement updateStmt = null;
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        //1.注册和加载驱动
        Class.forName("com.mysql.jdbc.Driver");
        conn = DriverManager.getConnection(url, username, pw);
        //conn = DriverManager.getConnection("jdbc:mysql://172.16.8.208:3306/bd_ads_test", "bdmyus", "CdHJ22#MyD");
        insertStmt = conn.prepareStatement("insert into monitoring_table (" +
                "tier, device_id,data_size,win_end_time,customer_id) values ('dws',?,?,?,?)");
        //updateStmt = conn.prepareStatement("update monitoring_table set data_size = data_size+1 WHERE tier = 'dws' ORDER BY win_end_time DESC LIMIT 1");
    }

    @Override
    public void invoke(Tuple3<Integer,String,String> value, Context context) {
        try {
        /* if(value.f0 == -1){
            //执行更新操作
            updateStmt.execute();
        }else{*/
            //执行插入操作
            insertStmt.setString(1, value.f1);
            insertStmt.setInt(2, value.f0);
            insertStmt.setString(3,value.f2);
            insertStmt.setString(4, value.f1);
            insertStmt.execute();
        //}
        }catch (Exception e){
        e.printStackTrace();
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        conn.close();
        insertStmt.close();
    }
}
