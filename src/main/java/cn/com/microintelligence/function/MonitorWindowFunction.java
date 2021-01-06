package cn.com.microintelligence.function;

import cn.com.microintelligence.bean.DwsProductAccepted;
import cn.com.microintelligence.utils.TimeUtils;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;

public class MonitorWindowFunction implements WindowFunction<Tuple3<String, Integer, String>, Tuple3<Integer, String, String>, Tuple, TimeWindow> {

    @Override
    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple3<String, Integer, String>> iterable, Collector<Tuple3<Integer, String, String>> collector) throws Exception {
        String customer_id = tuple.getField(0);
        //累加count值
        int sum = 0;
        for (Tuple3<String, Integer, String> value : iterable) {
            sum += value.f1;
        }
        //将累加值sum和窗口结束时间发送到自定义jdbc sink
        collector.collect(new Tuple3<Integer, String, String>(sum, customer_id, TimeUtils.long2strDate(timeWindow.getEnd() / 1000)));
    }

}
