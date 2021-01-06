package cn.com.microintelligence.sink;

import cn.com.microintelligence.bean.*;
import cn.com.microintelligence.common.AppConstant;
import cn.com.microintelligence.common.TimeFormat;
import cn.com.microintelligence.utils.JdbcMonthUtil;
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
public class MysqlMonthSink extends RichSinkFunction<Tuple2<JsonObject,String>> {
    private String jdbc_prop;
    private HikariDataSource hds = null;
    private Connection conn = null;
    private Statement stmt = null;
    public MysqlMonthSink(String jdbc_prop) {
        this.jdbc_prop = jdbc_prop;
    }
    public MysqlMonthSink(){

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
        String year = timeFormat.getYear();
        String month = timeFormat.getMonth();
        conn = getConnection();
        stmt = conn.createStatement();
        switch (value.f1) {
            case AppConstant.DWS_PRODUCT_ACCEPTED:
                DwsProductAccepted dwsProductAccepted = gson.fromJson(value.f0.toString(),DwsProductAccepted.class);
            try {
                ProductReportRs productReportRs = JdbcMonthUtil.checkDataAndGetValue(stmt, dwsProductAccepted, year, month);
                if (productReportRs.isExits()) {
                    JdbcMonthUtil.updateDwsProductAccepted(stmt, productReportRs, dwsProductAccepted, year, month, time);
                } else {
                    JdbcMonthUtil.insertDwsProductAccepted(stmt, dwsProductAccepted, year, month, time);
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
                ProductReportRs productReportRs = JdbcMonthUtil.checkDataAndGetValue(stmt, dwsProductRejects, year, month);
                if (productReportRs.isExits()) {
                    JdbcMonthUtil.updateDwsProductRejects(stmt, productReportRs, dwsProductRejects, year, month, time);
                } else {
                    JdbcMonthUtil.insertDwsProductRejects(stmt, dwsProductRejects, year, month, time);
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
                ProductDefectRs productDefectRs = JdbcMonthUtil.checkDataAndGetValueDefect(stmt, dwsProductDefect, year, month);
                if (productDefectRs.isExits()) {
                    JdbcMonthUtil.updateDwsProductDefect(stmt, productDefectRs, dwsProductDefect, year, month, time);
                } else {
                    JdbcMonthUtil.insertDwsProductDefect(stmt, dwsProductDefect, year, month, time);
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
