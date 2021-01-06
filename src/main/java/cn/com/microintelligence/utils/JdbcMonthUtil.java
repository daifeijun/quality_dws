package cn.com.microintelligence.utils;

import IdGenerator.IdWorker;
import cn.com.microintelligence.bean.*;
import org.apache.commons.codec.digest.DigestUtils;
import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author daifeijun
 */
public class JdbcMonthUtil implements Serializable {
    private static final String table_name = "ads_product_report_month";
    private static final String table_name_defect = "ads_product_defect_analyze_month";

    public static ProductReportRs checkDataAndGetValue(Statement stmt, DwsProductAccepted dwsProductAccepted, String year,String month) throws SQLException {
        String md5 = DigestUtils.md5Hex(dwsProductAccepted.getProduct_id()+dwsProductAccepted.getProduction_line_id()+
                dwsProductAccepted.getProcess_id()+dwsProductAccepted.getBatch_no()+dwsProductAccepted.getShift_id()+
                year+month+dwsProductAccepted.getCustomer_id());
        ProductReportRs reportRs = new ProductReportRs();
        String sql = "select i_good_product_num,i_rework_num,i_scrap_num,i_product_num from "  + table_name +
                " where i_customer_id='" +dwsProductAccepted.getCustomer_id()+ "' and md5_index='" +md5+ "'";
        ResultSet result = stmt.executeQuery(sql);
        while (result.next()) {
            reportRs = new ProductReportRs(
                    result.getLong("i_good_product_num"),
                    result.getLong("i_rework_num"),
                    result.getLong("i_scrap_num"),
                    result.getLong("i_product_num"),
                    true
            );
        }
        return reportRs;
    }

    public static void updateDwsProductAccepted(Statement stmt, ProductReportRs reportRs, DwsProductAccepted dwsProductAccepted, String year, String month,String time) throws SQLException {
        String md5 = DigestUtils.md5Hex(dwsProductAccepted.getProduct_id()+dwsProductAccepted.getProduction_line_id()+
                dwsProductAccepted.getProcess_id()+dwsProductAccepted.getBatch_no()+dwsProductAccepted.getShift_id()+
                year+month+dwsProductAccepted.getCustomer_id());
        long i_good_product_num = reportRs.getI_good_product_num() + Long.parseLong(dwsProductAccepted.getGood_product_num());
        long i_product_num = reportRs.getI_product_num() + Long.parseLong(dwsProductAccepted.getGood_product_num());
        String sql = "update "  + table_name +
                " set i_good_product_num = " + i_good_product_num +
                " , i_product_num = " + i_product_num +
                " , dt_detection_date_time = '" + time + "'" +
                " , dt_create_time = now()" +
                " where i_customer_id='" +dwsProductAccepted.getCustomer_id()+ "' and md5_index='" +md5+ "'";
        stmt.executeUpdate(sql);
    }

    public static void insertDwsProductAccepted(Statement stmt, DwsProductAccepted dwsProductAccepted,
                                                String year, String month, String time) throws SQLException {
        String md5 = DigestUtils.md5Hex(dwsProductAccepted.getProduct_id()+dwsProductAccepted.getProduction_line_id()+
                dwsProductAccepted.getProcess_id()+dwsProductAccepted.getBatch_no()+dwsProductAccepted.getShift_id()+
                year+month+dwsProductAccepted.getCustomer_id());
        String sql = "insert into "  + table_name + " ( i_business_id,i_product_id,i_product_name, i_production_line_id, i_production_line_name,i_process_id, i_process_name,i_batch_id,i_shift_id, i_shift_name,year, month, " +
                "dt_detection_date_time, i_good_product_num, i_rework_num,i_scrap_num,i_defective_num,i_product_num,i_customer_id,i_detection_type,md5_index) values ( " +
                "'" + IdWorker.getId() + "', " +
                "'" + dwsProductAccepted.getProduct_id() + "', " +
                "'" + dwsProductAccepted.getProduct_name() + "', " +
                "'" + dwsProductAccepted.getProduction_line_id() + "', " +
                "'" + dwsProductAccepted.getProduction_line_name() + "', " +
                "'" + dwsProductAccepted.getProcess_id() + "', " +
                "'" + dwsProductAccepted.getProcess_name() + "', " +
                "\"" + dwsProductAccepted.getBatch_no() + "\", " +
                "'" + dwsProductAccepted.getShift_id() + "', " +
                "'" + dwsProductAccepted.getShift_name() + "', " +
                "'" + year + "', " +
                "'" + month + "', " +
                "'" + time + "', " +
                "" + dwsProductAccepted.getGood_product_num() + ", " +
                "" + 0 + ", " +
                "" + 0 + ", " +
                "" + 0 + ", " +
                "" + dwsProductAccepted.getGood_product_num() + ", " +
                "'" + dwsProductAccepted.getCustomer_id() + "', " +
                "" + 1 + ",'"+md5+ "')";
        stmt.executeUpdate(sql);
    }

    public static ProductReportRs checkDataAndGetValue(Statement stmt, DwsProductRejects dwsProductRejects, String year, String month) throws SQLException {
        String md5 = DigestUtils.md5Hex(dwsProductRejects.getProduct_id()+dwsProductRejects.getProduction_line_id()+
                dwsProductRejects.getProcess_id()+dwsProductRejects.getBatch_no()+dwsProductRejects.getShift_id()+
                year+month+dwsProductRejects.getCustomer_id());
        ProductReportRs reportRs = new ProductReportRs();
        String sql = "select i_good_product_num,i_rework_num,i_scrap_num,i_product_num from "  + table_name +
                " where i_customer_id='" +dwsProductRejects.getCustomer_id()+ "' and md5_index='" +md5+ "'";
        ResultSet result = stmt.executeQuery(sql);
        while (result.next()) {
            reportRs = new ProductReportRs(
                    result.getLong("i_good_product_num"),
                    result.getLong("i_rework_num"),
                    result.getLong("i_scrap_num"),
                    result.getLong("i_product_num"),
                    true
            );
        }
        return reportRs;
    }

    public static void updateDwsProductRejects(Statement stmt, ProductReportRs reportRs, DwsProductRejects dwsProductRejects, String year, String month,String time) throws SQLException {
        String md5 = DigestUtils.md5Hex(dwsProductRejects.getProduct_id()+dwsProductRejects.getProduction_line_id()+
                dwsProductRejects.getProcess_id()+dwsProductRejects.getBatch_no()+dwsProductRejects.getShift_id()+
                year+month+dwsProductRejects.getCustomer_id());
        long i_scrap_num = reportRs.getI_scrap_num();
        long i_rework_num = reportRs.getI_rework_num();
        if("1".equals(dwsProductRejects.getProduct_grade_level())){
            i_scrap_num = i_scrap_num + Long.parseLong(dwsProductRejects.getProduct_defect_num());
        }else{
            i_rework_num = i_rework_num + Long.parseLong(dwsProductRejects.getProduct_defect_num());
        }
        long i_product_num = reportRs.getI_product_num() + Long.parseLong(dwsProductRejects.getProduct_defect_num());
        String sql = "update "  + table_name +
                " set i_scrap_num = "+i_scrap_num+",i_rework_num = "+i_rework_num+",i_product_num = " + i_product_num +
                " , dt_detection_date_time = '" + time + "'" +
                " , dt_create_time = now()" +
                " where i_customer_id='" +dwsProductRejects.getCustomer_id()+ "' and md5_index='" +md5+ "'";
        stmt.executeUpdate(sql);
    }

    public static void insertDwsProductRejects(Statement stmt, DwsProductRejects dwsProductRejects,
                                               String year, String month, String time) throws SQLException {
        long i_scrap_num = 0;
        long i_rework_num = 0;
        if("1".equals(dwsProductRejects.getProduct_grade_level())){
            i_scrap_num = Long.parseLong(dwsProductRejects.getProduct_defect_num());
        }else{
            i_rework_num = Long.parseLong(dwsProductRejects.getProduct_defect_num());
        }
        String md5 = DigestUtils.md5Hex(dwsProductRejects.getProduct_id()+dwsProductRejects.getProduction_line_id()+
                dwsProductRejects.getProcess_id()+dwsProductRejects.getBatch_no()+dwsProductRejects.getShift_id()+
                year+month+dwsProductRejects.getCustomer_id());
        String sql = "insert into "  + table_name + " ( i_business_id,i_product_id,i_product_name, i_production_line_id,i_production_line_name, i_process_id,i_process_name, i_batch_id, i_shift_id,i_shift_name,year, month, " +
                "dt_detection_date_time, i_good_product_num, i_rework_num,i_scrap_num,i_defective_num,i_product_num,i_customer_id,i_detection_type,md5_index) values ( " +
                "'" + IdWorker.getId() + "', " +
                "'" + dwsProductRejects.getProduct_id() + "', " +
                "'" + dwsProductRejects.getProduct_name() + "', " +
                "'" + dwsProductRejects.getProduction_line_id() + "', " +
                "'" + dwsProductRejects.getProduction_line_name() + "', " +
                "'" + dwsProductRejects.getProcess_id() + "', " +
                "'" + dwsProductRejects.getProcess_name() + "', " +
                "\"" + dwsProductRejects.getBatch_no() + "\", " +
                "'" + dwsProductRejects.getShift_id() + "', " +
                "'" + dwsProductRejects.getShift_name() + "', " +
                "'" + year + "', " +
                "'" + month + "', " +
                "'" + time + "', " +
                "" + 0 + ", " +
                "" + i_rework_num  + ", " +
                "" + i_scrap_num+ ", " +
                "" + 0 + ", " +
                "" + Long.parseLong(dwsProductRejects.getProduct_defect_num()) + ", " +
                "'" + dwsProductRejects.getCustomer_id() + "', " +
                "" + 1 + ",'"+md5+ "')";
        stmt.executeUpdate(sql);
    }

    public static ProductDefectRs checkDataAndGetValueDefect(Statement stmt, DwsProductDefect dwsProductDefect, String year, String month) throws SQLException {
        String md5 = DigestUtils.md5Hex(dwsProductDefect.getProduct_id()+ dwsProductDefect.getProduction_line_id()+
                dwsProductDefect.getProcess_id()+dwsProductDefect.getBatch_no()+dwsProductDefect.getShift_id()+
                year+month+dwsProductDefect.getProduct_grade_code()+dwsProductDefect.getCustomer_id());
        ProductDefectRs reportRs = new ProductDefectRs();
        String sql = "select i_defect_fqs from "  + table_name_defect +
                " where i_customer_id='" +dwsProductDefect.getCustomer_id()+ "' and md5_index='" +md5+ "'";
        ResultSet result = stmt.executeQuery(sql);
        while (result.next()) {
            reportRs = new ProductDefectRs(
                    result.getLong("i_defect_fqs"),
                    true
            );
        }
        return reportRs;
    }

    public static void updateDwsProductDefect(Statement stmt, ProductDefectRs reportRs, DwsProductDefect dwsProductDefect, String year, String month,String time) throws SQLException {
        String md5 = DigestUtils.md5Hex(dwsProductDefect.getProduct_id()+ dwsProductDefect.getProduction_line_id()+
                dwsProductDefect.getProcess_id()+dwsProductDefect.getBatch_no()+dwsProductDefect.getShift_id()+
                year+month+dwsProductDefect.getProduct_grade_code()+dwsProductDefect.getCustomer_id());
        long i_defect_fqs = reportRs.getI_defect_fqs() + Long.parseLong(dwsProductDefect.getDefect_fqs());
        String sql = "update "  + table_name_defect +
                " set i_defect_fqs = " + i_defect_fqs +
                " , dt_detection_date_time = '" + time + "'" +
                " , dt_create_time = now()" +
                " where i_customer_id='" +dwsProductDefect.getCustomer_id()+ "' and md5_index='" +md5+ "'";
        stmt.executeUpdate(sql);
    }

    public static void insertDwsProductDefect(Statement stmt, DwsProductDefect dwsProductDefect, String year, String month, String time) throws SQLException {
        String md5 = DigestUtils.md5Hex(dwsProductDefect.getProduct_id()+ dwsProductDefect.getProduction_line_id()+
                dwsProductDefect.getProcess_id()+dwsProductDefect.getBatch_no()+dwsProductDefect.getShift_id()+
                year+month+dwsProductDefect.getProduct_grade_code()+dwsProductDefect.getCustomer_id());
        String sql = "insert into "  + table_name_defect + " (i_business_id,i_product_id,i_product_name,i_production_line_id,i_production_line_name,i_process_id,i_process_name,i_batch_id,i_shift_id,i_shift_name," +
                "year,month,dt_detection_date_time,i_product_defect_id,i_product_defect_name,i_defect_fqs,i_customer_id,md5_index) values ( " +
                "'" + IdWorker.getId() + "', " +
                "'" + dwsProductDefect.getProduct_id() + "', " +
                "'" + dwsProductDefect.getProduct_name() + "', " +
                "'" + dwsProductDefect.getProduction_line_id() + "', " +
                "'" + dwsProductDefect.getProduction_line_name() + "', " +
                "'" + dwsProductDefect.getProcess_id() + "', " +
                "'" + dwsProductDefect.getProcess_name() + "', " +
                "\"" + dwsProductDefect.getBatch_no() + "\", " +
                "'" + dwsProductDefect.getShift_id() + "', " +
                "'" + dwsProductDefect.getShift_name() + "', " +
                "'" + year + "', " +
                "'" + month + "', " +
                "'" + time + "', " +
                "" + dwsProductDefect.getProduct_grade_code() + ", " +
                "'" + dwsProductDefect.getProduct_grade_name() + "'," +
                "" + dwsProductDefect.getDefect_fqs() + ", " +
                "'" + dwsProductDefect.getCustomer_id() + "', '"+md5+ "')";
        stmt.executeUpdate(sql);
    }

}
