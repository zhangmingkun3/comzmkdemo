package FlinkStudyDemo.watermark;

import FlinkStudyDemo.bean.UserEvent;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

public class CustomWatermarkExtractor extends BoundedOutOfOrdernessTimestampExtractor<UserEvent> {


    public CustomWatermarkExtractor(Time maxOutOfOrderness) {
        super(maxOutOfOrderness);
    }

    @Override
    public long extractTimestamp(UserEvent element) {
        return element.getEventTimestamp();
    }
}
