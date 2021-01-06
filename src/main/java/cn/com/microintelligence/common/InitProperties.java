package cn.com.microintelligence.common;

import com.ctrip.framework.apollo.Config;
import com.ctrip.framework.apollo.ConfigService;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;

/**
 * Creted By DFJ ON 2020-04-22
 **/
public class InitProperties {
    public static String kafkaHost;//kafka的host
    public static String dwsTopic;//dws层topic
    public static String dwsGroup;//dws层groupid
    public static String monTopic;//监控topic
    public static String hdfsPath;//checkpoint hdfs目录
    //public static String adsDatabase;//ads层数据库名 bd_ads/bd_ads_test
    public static String bd_monitor_url;
    public static String mysql_url;
    public static String mysql_pw;
    public static String mysql_username;
    public static String mycat_url;
    public static String mycat_pw;
    public static String mycat_username;
    public static String jdbc_prop;
    public static String ns;


    public static void extract(String[] args, StreamExecutionEnvironment env){
        ParameterTool parameters = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(parameters);
        String envPath = parameters.get("ns", null);
        Config config = ConfigService.getConfig(envPath);
        ns = envPath;
        kafkaHost = config.getProperty("kafka_host", null);
        dwsTopic = config.getProperty("dws_topic", null);
        dwsGroup = config.getProperty("dws_group_id", null);
        monTopic = config.getProperty("mon_topic", null);
        hdfsPath = config.getProperty("dws_chk_hdfs", null);
        bd_monitor_url = config.getProperty("bd_monitor_url", null);
        mysql_url = config.getProperty("mysql_url", null);
        mysql_pw = config.getProperty("mysql_pw", null);
        jdbc_prop = config.getProperty("jdbc_prop", null);
        mysql_username = config.getProperty("mysql_username", null);
        mycat_url = config.getProperty("mycat_url", null);
        mycat_pw = config.getProperty("mycat_pw", null);
        mycat_username = config.getProperty("mycat_username", null);
    }
    public static void initEnv(StreamExecutionEnvironment env) throws IOException {
        env.enableCheckpointing(60000L);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConfig.setMinPauseBetweenCheckpoints(30000L);
        checkpointConfig.setCheckpointTimeout(10000L);
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        StateBackend stateBackend = new RocksDBStateBackend(InitProperties.hdfsPath,true);
        env.setStateBackend(stateBackend);
        //自动重启重试次数
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                10,
                org.apache.flink.api.common.time.Time.minutes(5),
                org.apache.flink.api.common.time.Time.seconds(15)
        ));
    }
}
