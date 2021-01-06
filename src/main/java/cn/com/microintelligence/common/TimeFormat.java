package cn.com.microintelligence.common;

import cn.com.microintelligence.utils.TimeUtils;
import com.google.gson.JsonObject;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class TimeFormat {
    public String date;
    public String dt_date;
    public String year;
    public String month;
    public String dt;
    public String dh;
    public String time;
    public TimeFormat(JsonObject json) {
        time = TimeUtils.str2Formatter(json.get("classfied_date_time").getAsString(), AppConstant.DATE_TYPE_MS, AppConstant.DATE_TYPE_SS);
        dt_date = json.get("dt_date").getAsString();
        date=TimeUtils.str2Formatter(dt_date, AppConstant.DATE_TYPE_SS, AppConstant.DATE_TYPE_DAY);
        year = TimeUtils.str2Formatter(dt_date, AppConstant.DATE_TYPE_SS, AppConstant.DATE_TYPE_YEAR);
        month = TimeUtils.str2Formatter(dt_date, AppConstant.DATE_TYPE_SS, AppConstant.DATE_TYPE_MONTH);
        dt = TimeUtils.str2Formatter(dt_date, AppConstant.DATE_TYPE_SS, AppConstant.DATE_TYPE_DT);
        dh = TimeUtils.str2Formatter(dt_date, AppConstant.DATE_TYPE_SS, AppConstant.DATE_TYPE_DH);
    }
}
