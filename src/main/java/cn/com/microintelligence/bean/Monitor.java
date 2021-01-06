package cn.com.microintelligence.bean;

import lombok.Getter;
import lombok.Setter;

/**
 * 监控对象实体类
 */
@Getter
@Setter
public class Monitor {
    public String tier;
    public String i_device_id;
    public int data_size;
    public String i_customer_id;
    public String win_end_time;
    public String data_type;

    public Monitor(String tier, String i_device_id, int data_size, String i_customer_id, String win_end_time,String data_type) {
        this.tier = tier;
        this.i_device_id = i_device_id;
        this.data_size = data_size;
        this.i_customer_id = i_customer_id;
        this.win_end_time = win_end_time;
        this.data_type = data_type;
    }
}
