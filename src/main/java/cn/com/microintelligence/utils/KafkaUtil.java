package cn.com.microintelligence.utils;

import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;

public class KafkaUtil {
    /**
     * 暴力解析:Alibaba fastjson
     * @param jsonStr
     * @return
     */
    public final static boolean isJSONValid(String jsonStr) {
        try {
            JSONObject.parseObject(jsonStr);
        } catch (JSONException ex) {
            try {
                JSONObject.parseArray(jsonStr);
            } catch (JSONException ex1) {
                return false;
            }
        }
        return true;
    }
}
