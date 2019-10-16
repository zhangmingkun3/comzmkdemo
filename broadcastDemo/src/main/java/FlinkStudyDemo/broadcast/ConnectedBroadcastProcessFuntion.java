package FlinkStudyDemo.broadcast;

import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * BroadcastConnectedStream调用process()方法，参数类型为KeyedBroadcastProcessFunction或者BroadcastProcessFunction
 *
 */

public class ConnectedBroadcastProcessFuntion extends KeyedBroadcastProcessFunction {
    @Override
    public void processElement(Object value, ReadOnlyContext ctx, Collector out) throws Exception {

    }

    @Override
    public void processBroadcastElement(Object value, Context ctx, Collector out) throws Exception {

    }
}
