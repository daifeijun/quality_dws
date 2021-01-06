package cn.com.microintelligence.function;

import cn.com.microintelligence.common.AppConstant;
import cn.com.microintelligence.utils.DateUtil;
import cn.com.microintelligence.utils.TimeUtils;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class MyAsyncFunction extends RichAsyncFunction<Tuple2<com.google.gson.JsonObject,String>, Tuple2<com.google.gson.JsonObject, String>> {
    private String url;
    private String user;
    private String pwd;

    public MyAsyncFunction(String url, String user, String pwd) {
        this.url = url;
        this.user = user;
        this.pwd = pwd;
    }

    public MyAsyncFunction() {
    }

    //ApolloConfigManager conf = ApolloConfigManager.getInstance();
    private transient SQLClient mysqlClient;
    private Cache<String, String> cache;
    @Override
    public void open(Configuration parameters) throws Exception {
        cache = Caffeine
                .newBuilder()
                .maximumSize(1025)
                .expireAfterAccess(60, TimeUnit.MINUTES) //设置缓存过期时间
                .build();
        JsonObject mySQLClientConfig = new JsonObject();
        mySQLClientConfig.put("url", url)
                .put("driver_class", "com.mysql.cj.jdbc.Driver")
                .put("max_pool_size", 20)
                .put("user", user)
                .put("max_idle_time", 1000)
                .put("password",pwd);

        VertxOptions vo = new VertxOptions();
        vo.setEventLoopPoolSize(10);
        vo.setWorkerPoolSize(20);

        Vertx vertx = Vertx.vertx(vo);
        // 创建Mysql客户端
        mysqlClient = JDBCClient.createNonShared(vertx, mySQLClientConfig);
        super.open(parameters);
    }

    @Override
    public void asyncInvoke(Tuple2<com.google.gson.JsonObject, String> value, ResultFuture<Tuple2<com.google.gson.JsonObject, String>> resultFuture) throws Exception {
        com.google.gson.JsonObject jsonObject= value.f0;
        //获得HH:mm格式
        String dt_detection_date_time = TimeUtils.str2Formatter(jsonObject.get("classfied_date_time").getAsString(), AppConstant.DATE_TYPE_MS, AppConstant.DATE_TYPE_HM);
        String i_customer_id = jsonObject.get("customer_id").getAsString();
        /*
         * 先在缓存中查找数据
         */
        String dim_value =cache.getIfPresent(i_customer_id+"|"+dt_detection_date_time);
        if (dim_value!=null) {
            //有班次信息直接添加
            jsonObject.addProperty("shift_id",dim_value.split("\\|")[0]);
            jsonObject.addProperty("shift_name",dim_value.split("\\|")[1]);
            jsonObject.addProperty("dt_date",DateUtil.getDate(jsonObject.get("classfied_date_time").getAsString(),Integer.parseInt(dim_value.split("\\|")[2])));
            resultFuture.complete(Collections.singleton(new Tuple2<com.google.gson.JsonObject, String>(jsonObject,value.f1)));
        }else {
            mysqlClient.getConnection(conn -> {
                if (conn.failed()) {
                    resultFuture.completeExceptionally(conn.cause());
                    return;
                }
                final SQLConnection sqlConnection = conn.result();
                //拼接查询语句
                String querySql =
                        "SELECT CAST(i_shift_id AS VARCHAR) as i_shift_id,shift_name,code,CAST(start_time AS VARCHAR) as start_time,CAST(end_time AS VARCHAR) as end_time FROM dim_shift WHERE i_customer_id= " + i_customer_id;
                //执行查询，并获取结果
                sqlConnection.query(querySql, res -> {
                    if (res.succeeded()) {
                        if (res.failed()) {
                            resultFuture.completeExceptionally(null);
                            return;
                        }
                        ResultSet result = res.result();
                        List<JsonObject> rows = result.getRows();
                        if (rows.size() <= 0) {//没查出信息，代表没有匹配到班次id
                            jsonObject.addProperty("shift_id","0");
                            resultFuture.complete(Collections.singleton(new Tuple2<com.google.gson.JsonObject, String>(jsonObject,value.f1)));
                        } else {
                            //结果返回
                            for (JsonObject row : rows) {
                                String i_shift_id = row.getString("i_shift_id");
                                String shift_name = row.getString("shift_name");
                                int code = row.getInteger("code");
                                String sourceTime = row.getString("start_time")+"-"+row.getString("end_time");
                                if(DateUtil.isInTime(sourceTime,dt_detection_date_time)){
                                    jsonObject.addProperty("shift_id",i_shift_id);
                                    jsonObject.addProperty("shift_name",shift_name);
                                    jsonObject.addProperty("dt_date",DateUtil.getDate(jsonObject.get("classfied_date_time").getAsString(),code));
                                    //再更新缓存
                                    cache.put(i_customer_id+"|"+dt_detection_date_time,i_shift_id+"|"+shift_name+"|"+code);
                                    resultFuture.complete(Collections.singleton(new Tuple2<com.google.gson.JsonObject, String>(jsonObject,value.f1)));
                                    break;
                                }
                            }
                        }
                    }else {
                        //执行失败，返回空
                        resultFuture.complete(null);
                        return;
                    }
                });
                //连接关闭
                sqlConnection.close(done -> {
                    if (done.failed()) {
                        throw new RuntimeException(done.cause());
                    }
                });
            });
        }
    }
}
