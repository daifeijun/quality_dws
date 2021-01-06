package cn.com.microintelligence.sink;

import cn.com.microintelligence.bean.*;
import cn.com.microintelligence.common.AppConstant;
import cn.com.microintelligence.common.TimeFormat;
import cn.com.microintelligence.utils.JdbcUtil;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import java.sql.*;

/**
 * @author daifeijun
 */
public class MysqlSink extends RichSinkFunction<Tuple2<JsonObject,String>> {
    private String jdbc_prop;
    private HikariDataSource hds = null;
    private Connection conn = null;
    private Statement stmt = null;
    public MysqlSink(String jdbc_prop) {
        this.jdbc_prop = jdbc_prop;
    }
    public MysqlSink() {
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
        String dh = timeFormat.getDh();
        conn = getConnection();
        stmt = conn.createStatement();
        switch (value.f1) {
        case AppConstant.DWS_PRODUCT_ACCEPTED:
            DwsProductAccepted dwsProductAccepted = gson.fromJson(value.f0.toString(),DwsProductAccepted.class);
            try {
                ProductReportRs productReportRs = JdbcUtil.checkDataAndGetValue(stmt, dwsProductAccepted, date, dh);
                if (productReportRs.isExits()) {
                    JdbcUtil.updateDwsProductAccepted(stmt, productReportRs, dwsProductAccepted, date, dh, time);
                } else {
                    JdbcUtil.insertDwsProductAccepted(stmt, dwsProductAccepted, date, year, month, dt, dh, time);
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
                ProductReportRs productReportRs = JdbcUtil.checkDataAndGetValue(stmt, dwsProductRejects, date, dh);
                if (productReportRs.isExits()) {
                    JdbcUtil.updateDwsProductRejects(stmt, productReportRs, dwsProductRejects, date, dh, time);
                } else {
                    JdbcUtil.insertDwsProductRejects(stmt, dwsProductRejects, date, year, month, dt, dh, time);
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
                ProductDefectRs productDefectRs = JdbcUtil.checkDataAndGetValueDefect(stmt, dwsProductDefect, date, dh);
                if (productDefectRs.isExits()) {
                    JdbcUtil.updateDwsProductDefect(stmt, productDefectRs, dwsProductDefect, date, dh, time);
                } else {
                    JdbcUtil.insertDwsProductDefect(stmt, dwsProductDefect, date, year, month, dt, dh, time);
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
