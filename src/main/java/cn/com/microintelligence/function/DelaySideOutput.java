package cn.com.microintelligence.function;

import cn.com.microintelligence.bean.DwsProductAccepted;
import cn.com.microintelligence.bean.DwsProductRejects;
import cn.com.microintelligence.common.AppConstant;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 侧输出流的工具类
 */
public class DelaySideOutput extends ProcessFunction<Tuple2<JsonObject, String>, Tuple2<JsonObject, String>> {
    @Override
    public void processElement(Tuple2<JsonObject, String> value, Context context, Collector<Tuple2<JsonObject, String>> out) throws Exception {
        Gson gson = new Gson();
        switch (value.f1) {
            case AppConstant.DELAY_DELAY_DWS_PRODUCT_ACCEPTED:
                context.output(AppConstant.OUTPUT_TAG_DELAY_DWS_PRODUCT_ACCEPTED, gson.fromJson(value.f0.toString(),DwsProductAccepted.class));
                break;
            case AppConstant.DELAY_DELAY_DWS_PRODUCT_REJECTS:
                context.output(AppConstant.OUTPUT_TAG_DELAY_DWS_PRODUCT_REJECTS, gson.fromJson(value.f0.toString(),DwsProductRejects.class));
                break;
            default:
                out.collect(value);
        }
    }
}

