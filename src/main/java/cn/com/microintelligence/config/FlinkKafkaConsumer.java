package cn.com.microintelligence.config;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/**
 * @author lucas
 */
public class FlinkKafkaConsumer<T> extends FlinkKafkaConsumer011<T> {

    private AbstractDeserialization<T> valueDeserializer;

    public FlinkKafkaConsumer(String topic, AbstractDeserialization<T> valueDeserializer, Properties props) {
        super(topic, valueDeserializer, props);
        this.valueDeserializer = valueDeserializer;
    }

    @Override
    public void run(SourceContext<T> sourceContext) throws Exception {
        valueDeserializer.setRuntimeContext(getRuntimeContext());
        valueDeserializer.initMetric();
        super.run(sourceContext);
    }
}
