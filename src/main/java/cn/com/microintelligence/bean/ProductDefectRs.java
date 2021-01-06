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
public class ProductDefectRs {
    private long i_defect_fqs;
    private boolean isExits = false;

    public ProductDefectRs(long i_defect_fqs,boolean isExits) {
        this.i_defect_fqs = i_defect_fqs;
        this.isExits = isExits;
    }

    public ProductDefectRs() {
    }
}
