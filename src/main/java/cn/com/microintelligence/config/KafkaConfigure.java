package cn.com.microintelligence.config;

import cn.com.microintelligence.common.AppConstant;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.JsonObject;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

import java.util.List;
import java.util.Optional;
import java.util.Properties;

/**
 * @author lucas
 */
public class KafkaConfigure implements java.io.Serializable {

    private Properties properties = null;

    public FlinkKafkaProducer011<String> getKafkaProducerTopic(String kafkaBootstrapServers, String kafkaGroupId, String kafkaTopic) {
        properties = new Properties();
        properties.setProperty(AppConstant.KAFKA_BOOTSTRAP_SERVERS, kafkaBootstrapServers);
        properties.setProperty(AppConstant.KAFKA_GROUP_ID, kafkaGroupId);
        return new FlinkKafkaProducer011<>(kafkaTopic, new SimpleStringSchema(), properties, Optional.ofNullable(null));
    }

    public FlinkKafkaConsumer<JsonObject> getKafkaConsumerTopic(String kafkaBootstrapServers, String kafkaGroupId, String offsetRest, String kafkaTopic) {
        properties = new Properties();
        properties.setProperty(AppConstant.KAFKA_BOOTSTRAP_SERVERS, kafkaBootstrapServers);
        properties.setProperty(AppConstant.KAFKA_GROUP_ID, kafkaGroupId);
        properties.setProperty(AppConstant.KAFKA_AUTO_OFFSET_RESET, offsetRest);
        return new FlinkKafkaConsumer<>(kafkaTopic, new ParseDeserialization(), properties);
    }

    public FlinkKafkaConsumer011<String> getKafkaConsumerOneTopic(String kafkaBootstrapServers, String kafkaGroupId, String offsetRest, String kafkaTopic) {
        properties = new Properties();
        properties.setProperty(AppConstant.KAFKA_BOOTSTRAP_SERVERS, kafkaBootstrapServers);
        properties.setProperty(AppConstant.KAFKA_GROUP_ID, kafkaGroupId);
        properties.setProperty(AppConstant.KAFKA_AUTO_OFFSET_RESET, offsetRest);
        return new FlinkKafkaConsumer011<>(kafkaTopic, new SimpleStringSchema(), properties);
    }

    public FlinkKafkaConsumer011<String> getKafkaConsumerListTopic(String kafkaBootstrapServers, String kafkaGroupId, String offsetRest, List<String> kafkaTopics) {
        properties = new Properties();
        properties.setProperty(AppConstant.KAFKA_BOOTSTRAP_SERVERS, kafkaBootstrapServers);
        properties.setProperty(AppConstant.KAFKA_GROUP_ID, kafkaGroupId);
        properties.setProperty(AppConstant.KAFKA_AUTO_OFFSET_RESET, offsetRest);
        return new FlinkKafkaConsumer011<>(kafkaTopics, new SimpleStringSchema(), properties);
    }

}
