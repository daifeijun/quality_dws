package cn.com.microintelligence.bean;


import lombok.Getter;
import lombok.Setter;

/**dws产品缺陷统计
 * bean
 */
@Getter
@Setter
public class DwsProductDefect {
    public String product_id;//产品id
    public String product_name;//产品名称
    public String production_line_id;//产线id
    public String production_line_name;//产线名称
    public String process_id;//工序id
    public String process_name;//工序名称
    public String batch_no;//批次编号
    public String shift_id;//班次id
    public String shift_name;//班次名称
    public String dt_date;//所属日期
    public String classfied_date_time;//检测时间
    public String product_grade_code;//缺陷id
    public String product_grade_name;//缺陷名称
    public String defect_fqs;//缺陷频次
    public String customer_id;//客户id
    public String dt_create_time;//数据处理时间
    public String data_type;//json字符串标识（使用表名）

    public DwsProductDefect(String product_id, String product_name, String production_line_id, String production_line_name, String process_id, String process_name, String batch_no, String shift_id, String shift_name, String dt_date, String classfied_date_time, String product_grade_code, String product_grade_name,String defect_fqs, String customer_id, String dt_create_time, String data_type) {
        this.product_id = product_id;
        this.product_name = product_name;
        this.production_line_id = production_line_id;
        this.production_line_name = production_line_name;
        this.process_id = process_id;
        this.process_name = process_name;
        this.batch_no = batch_no;
        this.shift_id = shift_id;
        this.shift_name = shift_name;
        this.dt_date = dt_date;
        this.classfied_date_time = classfied_date_time;
        this.product_grade_code = product_grade_code;
        this.product_grade_name=product_grade_name;
        this.defect_fqs = defect_fqs;
        this.customer_id = customer_id;
        this.dt_create_time = dt_create_time;
        this.data_type = data_type;
    }

    public DwsProductDefect() {
    }
}
