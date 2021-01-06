package cn.com.microintelligence.function;

import cn.com.microintelligence.bean.DwsProductAccepted;
import cn.com.microintelligence.bean.DwsProductDefect;
import cn.com.microintelligence.bean.DwsProductRejects;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;

public class DefectWindowFunction implements WindowFunction<DwsProductRejects, DwsProductDefect, Tuple, TimeWindow> {

    @Override
    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<DwsProductRejects> iterable, Collector<DwsProductDefect> collector) throws Exception {
        //得到检测时间 格式为 年-月-日 时
        String time = tuple.getField(9);
        //如果日期小时数与当前一致，将当前时间设置为检测试剂，否则拼接分钟秒数 59:59
        String nowTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date());
        if(time.equals(nowTime.split(":")[0])){
            time = nowTime;
        }else{
            time = time+":59:59.000";
        }
        //累加count值
        int sum = 0;
        for (DwsProductRejects value : iterable) {
            sum += 1;
        }
        collector.collect(new DwsProductDefect(tuple.getField(0),tuple.getField(1),
                tuple.getField(2),tuple.getField(3),tuple.getField(4),tuple.getField(5),
                tuple.getField(6),tuple.getField(7),tuple.getField(8),
                "",time,tuple.getField(10),tuple.getField(11),String.valueOf(sum),tuple.getField(12),
                "","dws_product_defect"
        ));
    }
}
