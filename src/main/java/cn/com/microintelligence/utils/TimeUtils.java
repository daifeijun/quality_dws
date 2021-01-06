package cn.com.microintelligence.utils;
import com.ctrip.framework.apollo.Config;
import com.ctrip.framework.apollo.ConfigService;
import org.apache.commons.lang3.StringUtils;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;

/**
 * @author lucas
 */
public class TimeUtils {

    public static void main(String[] args) throws Exception {
        //somePublicNamespace   我们从arg 命令行参数获取就可以
        String somePublicNamespace = "dev.BigdataDev";

        Config config = ConfigService.getConfig(somePublicNamespace);
        String someKey = "mq_chk_hdfs";
        String someDefaultValue = "默认的value";
        String value = config.getProperty(someKey, someDefaultValue);
        System.out.println(value);
        //System.out.println(getTimestamp());

    }

    public static String str2Formatter(String time, String beforeFormatter, String afterFormatter) {
        return LocalDateTime.parse(time, DateTimeFormatter.ofPattern(beforeFormatter)).format(DateTimeFormatter.ofPattern(afterFormatter));
    }

    public static Long getCurrentFormatterTime() {
        return LocalDateTime.now().atZone(ZoneId.of("Asia/Shanghai")).toEpochSecond() * 1000;
    }

    public static String getCurrentFormatterTime(String formatter) {
        return LocalDateTime.now(ZoneId.of("Asia/Shanghai")).format(DateTimeFormatter.ofPattern(formatter));
    }

    public static long strDate2Long(String time, String formatter) {
        long timestamp = 0L;
        try {
            if (StringUtils.isBlank(time) || StringUtils.isBlank(formatter)) {
                return timestamp;
            }
            timestamp = LocalDateTime.parse(time, DateTimeFormatter.ofPattern(formatter)).atZone(ZoneId.of("Asia/Shanghai")).toEpochSecond();
            return timestamp * 1000L;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return timestamp;
    }

    /**
     * 因为 json 转时间的原因，时区 +16， fastjson 会 -8
     *
     * @param l
     * @param formatter
     * @return
     */
    public static String long2strDate(long l, String formatter) {
        String time = null;
        try {
            if (StringUtils.isBlank(formatter)) {
                return time;
            }
            Instant instant = Instant.ofEpochMilli(l);
            LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, ZoneId.of("Asia/Shanghai"));
            time = localDateTime.format(DateTimeFormatter.ofPattern(formatter));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return time;
    }

    public static Boolean getDate() {
        Date now;
        Calendar c = Calendar.getInstance();
        now = new Date(c.get(Calendar.YEAR), c.get(Calendar.MONTH) + 1, c.get(Calendar.DAY_OF_MONTH));
        Date date1 = new Date(2019, 6, 22);


        return date1.after(now);
    }

    public static String getChangrTime(String it) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long It = Long.valueOf(it);
        Date date = new Date(It * 1000);
//日期格式化
        String res = simpleDateFormat.format(date);
        return res;
    }

    public static String getChangrTime1() {

        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
        Date date = new Date(System.currentTimeMillis());
        return formatter.format(date);
    }

    public static String getTimestamp() {
//        //1578364621
//        String da = getChangrTime("1578365335");//1578365335
//        long time = (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).parse(da, new ParsePosition(0)).getTime() / 1000;
        /*SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = new Date(System.currentTimeMillis());*/
        return LocalDateTime.now(ZoneId.of("Asia/Shanghai")).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }

    public static long gettimestamp(String time) {
        String formatter = "yyyy-MM-dd HH:mm:ss.SSS";
        long ts = 0;
        long timestamp = 0L;
        try {
            if (StringUtils.isBlank(time) || StringUtils.isBlank(formatter)) {
//                LOG.error("strDate2Long 方法 {}", AppConstant.DATE_TRANSFER_LONG_EMPTY_ERR);
                return timestamp;
            }
            timestamp = LocalDateTime.parse(time, DateTimeFormatter.ofPattern(formatter)).toEpochSecond(ZoneOffset.ofHours(8));
            return timestamp * 1000L;
        } catch (Exception e) {
//            LOG.error("strDate2Long 方法 {}", AppConstant.DATE_TRANSFER_LONG_ERR);DATE_TRANSFER_LONG_ERR
        }
        return timestamp;
    }

    public static String long2strDate(long l) {
        String formatter = "yyyy-MM-dd HH:mm:ss";
        String time = null;
        try {
            if (StringUtils.isBlank(formatter)) {
                return time;
            }
            LocalDateTime localDateTime = LocalDateTime.ofEpochSecond(l, 0, ZoneOffset.of("+8"));
            time = localDateTime.format(DateTimeFormatter.ofPattern(formatter));
        } catch (Exception e) {
        }
        return time;
    }

}
