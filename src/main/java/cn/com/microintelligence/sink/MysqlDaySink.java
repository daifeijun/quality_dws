package cn.com.microintelligence.sink;

import cn.com.microintelligence.bean.*;
import cn.com.microintelligence.common.AppConstant;
import cn.com.microintelligence.common.TimeFormat;
import cn.com.microintelligence.utils.JdbcDayUtil;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author daifeijun
 */
public class MysqlDaySink extends RichSinkFunction<Tuple2<JsonObject,String>> {
    private String jdbc_prop;
    private HikariDataSource hds = null;
    private Connection conn = null;
    private Statement stmt = null;
    public MysqlDaySink(String jdbc_prop) {
        this.jdbc_prop = jdbc_prop;
    }
    public MysqlDaySink(){

    }
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        HikariConfig conf = new HikariConfig(jdbc_prop);
        hds = new HikariDataSource(conf);
    }

    public Connection getConnection() throws SQLException {
        return hds.getConnection();
    }
    @Override
    public void invoke(Tuple2<JsonObject,String> value, Context context) throws Exception {
        Gson gson = new Gson();
        TimeFormat timeFormat = new TimeFormat(value.f0);
        String time = timeFormat.getTime();
        String date = timeFormat.getDate();
        String year = timeFormat.getYear();
        String month = timeFormat.getMonth();
        String dt = timeFormat.getDt();
        conn = getConnection();
        stmt = conn.createStatement();
        switch (value.f1) {
            case AppConstant.DWS_PRODUCT_ACCEPTED:
                DwsProductAccepted dwsProductAccepted = gson.fromJson(value.f0.toString(),DwsProductAccepted.class);
            try {
                ProductReportRs productReportRs = JdbcDayUtil.checkDataAndGetValue(stmt, dwsProductAccepted, date);
                if (productReportRs.isExits()) {
                    JdbcDayUtil.updateDwsProductAccepted(stmt, productReportRs, dwsProductAccepted, date, time);
                } else {
                    JdbcDayUtil.insertDwsProductAccepted(stmt, dwsProductAccepted, date, year, month, dt, time);
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (conn != null) {
                    conn.close();
                }
            }
            break;
            case AppConstant.DWS_PRODUCT_REJECTS:
                DwsProductRejects dwsProductRejects = gson.fromJson(value.f0.toString(),DwsProductRejects.class);
            try {
                ProductReportRs productReportRs = JdbcDayUtil.checkDataAndGetValue(stmt, dwsProductRejects, date);
                if (productReportRs.isExits()) {
                    JdbcDayUtil.updateDwsProductRejects(stmt, productReportRs, dwsProductRejects, date, time);
                } else {
                    JdbcDayUtil.insertDwsProductRejects(stmt, dwsProductRejects, date, year, month, dt, time);
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (conn != null) {
                    conn.close();
                }
            }
                break;
            case AppConstant.DWS_PRODUCT_DEFECT:
                DwsProductDefect dwsProductDefect = gson.fromJson(value.f0.toString(),DwsProductDefect.class);
            try {
                ProductDefectRs productDefectRs = JdbcDayUtil.checkDataAndGetValueDefect(stmt, dwsProductDefect, date);
                if (productDefectRs.isExits()) {
                    JdbcDayUtil.updateDwsProductDefect(stmt, productDefectRs, dwsProductDefect, date, time);
                } else {
                    JdbcDayUtil.insertDwsProductDefect(stmt, dwsProductDefect, date, year, month, dt, time);
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (conn != null) {
                    conn.close();
                }
            }
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (conn != null) {
            conn.close();
        }
    }
}
