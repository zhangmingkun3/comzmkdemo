package FlinkStudyDemo.watermark;

import FlinkStudyDemo.bean.UserEvent;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

public class TimestampsAndWatermarks implements AssignerWithPeriodicWatermarks<UserEvent> {

    //乱序时间
    private final long maxOutOfOrderness = 3500;
    private long currentMaxTimestamp;

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
    }

    @Override
    public long extractTimestamp(UserEvent element, long previousElementTimestamp) {
        long timestamp = element.getEventTimestamp();
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
        return currentMaxTimestamp;
    }
}
