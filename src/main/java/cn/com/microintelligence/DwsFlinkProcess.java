package cn.com.microintelligence;

import cn.com.microintelligence.bean.DwsProductAccepted;
import cn.com.microintelligence.bean.DwsProductRejects;
import cn.com.microintelligence.bean.Monitor;
import cn.com.microintelligence.common.AppConstant;
import cn.com.microintelligence.common.InitProperties;
import cn.com.microintelligence.config.FlinkKafkaConsumer;
import cn.com.microintelligence.config.KafkaConfigure;
import cn.com.microintelligence.function.*;
import cn.com.microintelligence.function.MyAsyncFunction;
import cn.com.microintelligence.sink.*;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import java.util.concurrent.TimeUnit;

/**
 * @descript 描述：质检机数据dws层
 * 接收dwd层通过kafka的topic传过来的json数据
 * 解析json，处理后将数据先传到kafka进行处理进入ads层的mysql数据库，并写入到hdfs中（hive）
 * @auther 戴飞俊
 * @time 2020/3/16 10:00:00
 */
public class DwsFlinkProcess {

    private static KafkaConfigure kafkaConfigure = new KafkaConfigure();
    public static void main(String[] args) throws Exception{

        //创建flink环境对象
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);//dev环境设置为1
        InitProperties.extract(args,env);
        //添加checkpoint
        InitProperties.initEnv(env);

        //得到kafka的stream
        FlinkKafkaConsumer<JsonObject> kafkaConsumerTopic = kafkaConfigure.getKafkaConsumerTopic(InitProperties.kafkaHost,
                InitProperties.dwsGroup, "latest", InitProperties.dwsTopic);
        //将取得的kafka的json数据,3层处理，1.ads层(mysql);2.延迟数据开窗计算重新打回topic;3.监控表(mysql&&kafka)
        //将kafka数据中正常数据和延迟数据分开
        SingleOutputStreamOperator<Tuple2<JsonObject, String>> mainStream = env.addSource(kafkaConsumerTopic).map(new MapFunction<JsonObject, Tuple2<JsonObject, String>>() {
            @Override
            public Tuple2<JsonObject, String> map(JsonObject value) throws Exception {
                return new Tuple2<>(value, value.get("data_type").getAsString());
            }
        }).process(new DelaySideOutput());
        //mainStream.print();
        //将主流中需要插入到mysql数据进行拆分
        DataStream<Tuple2<JsonObject,String>> sinkStream = AsyncDataStream.unorderedWait(mainStream,new MyAsyncFunction(InitProperties.mycat_url,InitProperties.mycat_username,InitProperties.mycat_pw),10000L,
                TimeUnit.MILLISECONDS,
                100).uid("async-id").setParallelism(1);//dev环境设置为1
        //sinkStream.print();
        sinkStream.addSink(new MysqlSink(InitProperties.jdbc_prop)).setParallelism(1).name("save_report").uid("dws_save_id");//InitProperties.mysql_url,InitProperties.mysql_username,InitProperties.mysql_pw
        sinkStream.addSink(new MysqlDaySink(InitProperties.jdbc_prop)).setParallelism(1).name("save_report_day").uid("dws_summary_day_id");
        sinkStream.addSink(new MysqlMonthSink(InitProperties.jdbc_prop)).setParallelism(1).name("save_report_month").uid("dws_summary_month_id");
        //2.延迟数据开窗计算重新打回topic
        //处理良品对象侧输出流
        DataStream<DwsProductAccepted> delayAcceptedStream = mainStream.getSideOutput(AppConstant.OUTPUT_TAG_DELAY_DWS_PRODUCT_ACCEPTED);
        DataStream<String> acceptedStream = delayAcceptedStream.map(DwsProductAccepted -> {
            DwsProductAccepted.setClassfied_date_time(DwsProductAccepted.getClassfied_date_time().split(":")[0]);
            return DwsProductAccepted;
        }).keyBy("product_id","product_name","production_line_id","production_line_name","process_id","process_name","batch_no","shift_id","shift_name","classfied_date_time","customer_id")
                .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
                .apply(new AcceptedWindowFunction()).map(DwsProductAccepted -> new Gson().toJson(DwsProductAccepted)).name("delay_to_accepted").uid("dws_delay_to_accepted");
        //处理非良品对象侧输出流
        DataStream<DwsProductRejects> delayRejectsStream = mainStream.getSideOutput(AppConstant.OUTPUT_TAG_DELAY_DWS_PRODUCT_REJECTS).map(DwsProductRejects -> {
            DwsProductRejects.setClassfied_date_time(DwsProductRejects.getClassfied_date_time().split(":")[0]);
            return DwsProductRejects;
        });
        //开窗统计非良品数量
        DataStream<String> rejectsStream = delayRejectsStream.keyBy("product_id","product_name","production_line_id","production_line_name","process_id","process_name","batch_no","shift_id","shift_name","classfied_date_time","product_grade_code","product_grade_name","product_grade_level","customer_id")
                .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
                .apply(new RejectsWindowFunction()).map(DwsProductRejects -> new Gson().toJson(DwsProductRejects)).name("delay_to_rejects").uid("dws_delay_to_rejects");
        //开窗统计缺陷频次
        DataStream<String> defectStream = delayRejectsStream.keyBy("product_id","product_name","production_line_id","production_line_name","process_id","process_name","batch_no","shift_id","shift_name","classfied_date_time","product_grade_code","product_grade_name","customer_id")
                .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
                .apply(new DefectWindowFunction()).map(DwsProductDefect -> new Gson().toJson(DwsProductDefect)).name("delay_to_defect").uid("dws_delay_to_defect");

        acceptedStream.union(rejectsStream,defectStream)
                .addSink(kafkaConfigure.getKafkaProducerTopic(InitProperties.kafkaHost,InitProperties.dwsGroup,InitProperties.dwsTopic)).name("dalay_to_kafka").uid("dws_delay_kafka");
        //3.sink到mysql和kafka进行监控
        //转换数据格式
        DataStream<Tuple3<String, Integer, String>> dataStream = mainStream.map(tuple -> {
            return new Tuple3<String, Integer, String>("dws_count", 1, tuple.f0.get("customer_id").getAsString());
        }).returns(Types.TUPLE(Types.STRING, Types.INT, Types.STRING)).name("data_change").uid("dws_monitor_change_id");
        //开窗计算
        DataStream<Tuple3<Integer, String, String>> monitorStream= dataStream.keyBy(2)//以key分组统计
                //设置一个窗口函数，时间定义为5分钟
                .window(TumblingProcessingTimeWindows.of(Time.minutes(5)))
                .apply(new MonitorWindowFunction()).name("monitor_count").uid("dws_monitor_window_id");
        //监控的数据写入mysql
        monitorStream.addSink(new MonitorJdbcSink()).setParallelism(1).name("save_monitor").uid("dws_monitor_mysql_id");
        //监控的数据写入对应的kafka
        monitorStream.map(value -> {return new Gson().toJson(new Monitor("dws", "", value.f0, value.f1, value.f2,"qc")); })
                .name("monitor_to_kafka").uid("dws_monitor_map_id").addSink(kafkaConfigure.getKafkaProducerTopic(InitProperties.kafkaHost,InitProperties.dwsGroup,InitProperties.monTopic)).uid("dws_monitor_kafka_id");
        try {
            env.execute("Flink Streaming Dws Run");
        } catch (Exception e) {
            System.out.println("flink 运行异常");
            //TODU
            //异常处理
            e.printStackTrace();
        }
    }

}
