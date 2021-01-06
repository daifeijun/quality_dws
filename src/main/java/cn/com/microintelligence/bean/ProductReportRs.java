package cn.com.microintelligence.bean;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * @author lucas
 */
@Getter
@Setter
@ToString
public class ProductReportRs implements java.io.Serializable{
    private long i_good_product_num;
    private long i_rework_num;
    private long i_scrap_num;
    private long i_product_num;
    private boolean isExits = false;

    public ProductReportRs() {
    }

    public ProductReportRs(long i_good_product_num, long i_rework_num, long i_scrap_num, long i_product_num, boolean isExits) {
        this.i_good_product_num = i_good_product_num;
        this.i_rework_num = i_rework_num;
        this.i_scrap_num = i_scrap_num;
        this.i_product_num = i_product_num;
        this.isExits = isExits;
    }

}
