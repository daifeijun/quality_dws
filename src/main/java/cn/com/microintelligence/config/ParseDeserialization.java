package cn.com.microintelligence.config;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class ParseDeserialization extends AbstractDeserialization<JsonObject> {
    @Override
    public JsonObject deserialize(byte[] message) {
        JsonObject jsonObj = new JsonObject();
        try {
            jsonObj = new JsonParser().parse(new String(message,"UTF-8")).getAsJsonObject();
            desNormalDataNum.inc();
            meter.markEvent();
        } catch (Exception e) {
            desDirtyDataNum.inc();
            e.printStackTrace();
        }
        return jsonObj;
    }
}
