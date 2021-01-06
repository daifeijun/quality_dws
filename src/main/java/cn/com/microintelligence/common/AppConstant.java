package cn.com.microintelligence.common;

import cn.com.microintelligence.bean.DwsProductAccepted;
import cn.com.microintelligence.bean.DwsProductRejects;
import cn.com.microintelligence.utils.TimeUtils;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.OutputTag;

/**
 * @author lucas
 */
public class AppConstant {

    /**
     * kafka 相关参数 keys
     */
    public static final String KAFKA_BOOTSTRAP_SERVERS = "bootstrap.servers";
    public static final String KAFKA_GROUP_ID = "group.id";
    public static final String KAFKA_AUTO_OFFSET_RESET = "auto.offset.reset";
    /**
     * 时间格式
     */
    public static final String DATE_TYPE_SS = "yyyy-MM-dd HH:mm:ss";
    public static final String DATE_TYPE_MS = "yyyy-MM-dd HH:mm:ss.SSS";
    public static final String DATE_TYPE_DAY = "yyyy-MM-dd";
    public static final String DATE_TYPE_YEAR = "yyyy";
    public static final String DATE_TYPE_MONTH = "MM";
    public static final String DATE_TYPE_DT = "dd";
    public static final String DATE_TYPE_DH = "HH";
    public static final String DATE_TYPE_HM = "HH:mm";
    public static final String DEFAULT_ZONE_ID = "Asia/Shanghai";

    public static final String DWS_PRODUCT_ACCEPTED = "dws_product_accepted";
    public static final String DWS_PRODUCT_REJECTS = "dws_product_rejects";
    public static final String DWS_PRODUCT_DEFECT = "dws_product_defect";
    public static final String DELAY_DELAY_DWS_PRODUCT_ACCEPTED = "delay_dws_product_accepted";
    public static final String DELAY_DELAY_DWS_PRODUCT_REJECTS = "delay_dws_product_rejects";

    public static final OutputTag<DwsProductAccepted> OUTPUT_TAG_DELAY_DWS_PRODUCT_ACCEPTED
            = new OutputTag<>(DELAY_DELAY_DWS_PRODUCT_ACCEPTED, TypeInformation.of(new TypeHint<DwsProductAccepted>() {}));

    public static final OutputTag<DwsProductRejects> OUTPUT_TAG_DELAY_DWS_PRODUCT_REJECTS
            = new OutputTag<>(DELAY_DELAY_DWS_PRODUCT_REJECTS, TypeInformation.of(new TypeHint<DwsProductRejects>() {}));
}
