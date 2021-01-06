package cn.com.microintelligence.config;

import lombok.Getter;
import lombok.Setter;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;

/**
 * @author lucas
 */
@Getter
@Setter
public abstract class AbstractDeserialization<T> extends AbstractDeserializationSchema<T> {

    private static final String DES_NORMAL_DATA_NAME = "dwsDesNormalDataNum";
    private static final String DES_DIRTY_DATA_NAME = "dwsDesDirtyDataNum";
    private static final String DES_METER_DATA_NAME = "dwsDesMeterDataNum";

    private RuntimeContext runtimeContext;
    protected transient Counter desNormalDataNum;
    protected transient Counter desDirtyDataNum;
    protected transient Meter meter;

    public void initMetric() {
        desNormalDataNum = runtimeContext.getMetricGroup().counter(DES_NORMAL_DATA_NAME);
        desDirtyDataNum = runtimeContext.getMetricGroup().counter(DES_DIRTY_DATA_NAME);
        meter = runtimeContext.getMetricGroup().meter(DES_METER_DATA_NAME, new DropwizardMeterWrapper(new com.codahale.metrics.Meter()));
    }
}
