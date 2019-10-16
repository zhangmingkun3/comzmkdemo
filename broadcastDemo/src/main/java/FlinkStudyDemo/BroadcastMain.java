package FlinkStudyDemo;

import FlinkStudyDemo.bean.*;
import FlinkStudyDemo.watermark.CustomWatermarkExtractor;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.calcite.shaded.com.google.common.collect.Maps;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
public class BroadcastMain {

    // 定义
    private static final MapStateDescriptor<String, Config> configStateDescriptor =
            new MapStateDescriptor<>(
                    "configBroadcastState",
                    BasicTypeInfo.STRING_TYPE_INFO,
                    TypeInformation.of(new TypeHint<Config>() {}));

    public static void main(String[] args) throws Exception {
        //  参数为 input-event-topic、input-config-topic、output-topic  以及与Kfka集群建立所必需的的 bootstrap.servers和zookeeper.connect
        log.info("Input args: " + Arrays.asList(args));
        // parse input arguments
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        if (parameterTool.getNumberOfParameters() < 5) {
            System.out.println("Missing parameters!\n" +
                    "Usage: Kafka --input-event-topic <topic> --input-config-topic <topic> --output-topic <topic> " +
                    "--bootstrap.servers <kafka brokers> " +
                    "--zookeeper.connect <zk quorum> --group.id <some id>");
            return;
        }

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStateBackend(new FsStateBackend(
                "hdfs://namenode01.td.com/flink-checkpoints/customer-purchase-behavior-tracker"));
        CheckpointConfig config = env.getCheckpointConfig();
        config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        config.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        config.setCheckpointInterval(TimeUnit.HOURS.toHours(1));
        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(parameterTool);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        //创建一个用来处理用户在App上操作行为事件的Stream，并且使用map进行转换，使用keyBy来对Stream进行分区
        final FlinkKafkaConsumer010<String> kafkaUserEventSource = new FlinkKafkaConsumer010<String>(
                parameterTool.getRequired("input-event-topic"),
                new SimpleStringSchema(), parameterTool.getProperties());

        //  (userEvent, userId)  属于同一个用户的操作行为事件会发送到同一个下游的Task中进行处理，这样可以在Task中完整地保存某个用户相关的状态信息，
        // 从而等到PURCHASE类型的购物操作事件到达后进行一次计算，如果满足配置条件则处理缓存的事件并输出最终结果。
        final KeyedStream<UserEvent,String> customerUserEventStream  =  env.addSource(kafkaUserEventSource)
                .map(new RichMapFunction<String, UserEvent>() {
                    @Override
                    public UserEvent map(String s) throws Exception {
                        return UserEvent.buildEvent(s);
                    }
                })
                .assignTimestampsAndWatermarks(new CustomWatermarkExtractor(Time.hours(24)))
                .keyBy(new KeySelector<UserEvent, String>() {
                    @Override
                    public String getKey(UserEvent userEvent) throws Exception {
                        return userEvent.getUserId();
                    }
                });

        // 创建配置事件Stream
        // 最后一行调用了broadcast()方法，用来指定要广播的状态变量，它在Flink程序运行时会发送到下游每个Task中，供Task读取并使用对应配置信息，下游Task可以根据该状态变量
        // 就可以获取到对应的配置值。参数值configStateDescriptor是一个MapStateDescriptor类型的对象，定义并初始化
        //  事件中的channel（渠道）字段作为Key，也就是不同渠道对应的配置是不同的，实现了对渠道配置的灵活性。而对应的Value则是定义的Config
        final FlinkKafkaConsumer010<String> kafkaConfigEventSource = new FlinkKafkaConsumer010<String>(
                parameterTool.getRequired("input-config-topic"),
                new SimpleStringSchema(), parameterTool.getProperties());
        final BroadcastStream<Config> configBroadcastStream = env.addSource(kafkaConfigEventSource)
                .map(new RichMapFunction<String, Config>() {
                    @Override
                    public Config map(String value) throws Exception {
                        return Config.buildConfig(value);
                    }
                })
                .broadcast(configStateDescriptor);


        // 连接两个Stream并实现计算处理
        final FlinkKafkaProducer010 kafkaProducer = new FlinkKafkaProducer010(
                parameterTool.getRequired("output-topic"),
                (KeyedSerializationSchema) new EvaluatedResultSchema(),
                parameterTool.getProperties());

        // connect above 2 streams
        DataStream<EvaluatedResult> connectedStream = customerUserEventStream
                .connect(configBroadcastStream)
                .process(new KeyedBroadcastProcessFunction<String, UserEvent, Config, EvaluatedResult>() {

                    // (channel, Map<uid, UserEventContainer>)
                    private final MapStateDescriptor<String, Map<String, UserEventContainer>> userMapStateDesc =
                            new MapStateDescriptor<>(
                                    "userEventContainerState",
                                    BasicTypeInfo.STRING_TYPE_INFO,
                                    new MapTypeInfo<>(String.class, UserEventContainer.class));

                    private Config defaultConfig;

                    @Override
                    public void processElement(UserEvent value, ReadOnlyContext ctx, Collector<EvaluatedResult> out) throws Exception {
                        String userId = value.getUserId();
                        String channel = value.getChannel();

                        EventType eventType = EventType.valueOf(value.getEventType());
                        Config config = ctx.getBroadcastState(configStateDescriptor).get(channel);
                        log.info("Read config: channel=" + channel + ", config=" + config);
                        if (Objects.isNull(config)) {
                            config = defaultConfig;
                        }

                        final MapState<String, Map<String, UserEventContainer>> state = getRuntimeContext().getMapState(userMapStateDesc);

                        // collect per-user events to the user map state
                        Map<String, UserEventContainer> userEventContainerMap = state.get(channel);
                        if (Objects.isNull(userEventContainerMap)) {
                            userEventContainerMap = Maps.newHashMap();
                            state.put(channel, userEventContainerMap);
                        }
                        if (!userEventContainerMap.containsKey(userId)) {
                            UserEventContainer container = new UserEventContainer();
                            container.setUserId(userId);
                            userEventContainerMap.put(userId, container);
                        }
                        userEventContainerMap.get(userId).getUserEvents().add(value);

                        // check whether a user purchase event arrives
                        // if true, then compute the purchase path length, and prepare to trigger predefined actions
                        if (eventType == EventType.PURCHASE) {
                            log.info("Receive a purchase event: " + value);
                            Optional<EvaluatedResult> result = compute(config, userEventContainerMap.get(userId));
                            result.ifPresent(r -> out.collect(result.get()));
                            // clear evaluated user's events
                            state.get(channel).remove(userId);
                        }
                    }

                    @Override
                    public void processBroadcastElement(Config value, Context ctx, Collector<EvaluatedResult> out) throws Exception {
                        String channel = value.getChannel();
                        BroadcastState<String, Config> state = ctx.getBroadcastState(configStateDescriptor);
                        final Config oldConfig = ctx.getBroadcastState(configStateDescriptor).get(channel);
                        if(state.contains(channel)) {
                            log.info("Configured channel exists: channel=" + channel);
                            log.info("Config detail: oldConfig=" + oldConfig + ", newConfig=" + value);
                        } else {
                            log.info("Config detail: defaultConfig=" + defaultConfig + ", newConfig=" + value);
                        }
                        // update config value for configKey
                        state.put(channel, value);
                    }
                });
        connectedStream.addSink(kafkaProducer);



        env.execute("UserPurchaseBehaviorTracker");

    }


    private static Optional<EvaluatedResult> compute(Config config, UserEventContainer container) {
        Optional<EvaluatedResult> result = Optional.empty();
        String channel = config.getChannel();
        int historyPurchaseTimes = config.getHistoryPurchaseTimes();
        int maxPurchasePathLength = config.getMaxPurchasePathLength();

        int purchasePathLen = container.getUserEvents().size();
        if (historyPurchaseTimes < 10 && purchasePathLen > maxPurchasePathLength) {
            // sort by event time
            container.getUserEvents().sort(Comparator.comparingLong(UserEvent::getEventTimestamp));

            final Map<String, Integer> stat = Maps.newHashMap();
            container.getUserEvents()
                    .stream()
                    .collect(Collectors.groupingBy(UserEvent::getEventType))
                    .forEach((eventType, events) -> stat.put(eventType, events.size()));

            final EvaluatedResult evaluatedResult = new EvaluatedResult();
            evaluatedResult.setUserId(container.getUserId());
            evaluatedResult.setChannel(channel);
            evaluatedResult.setEventTypeCounts(stat);
            evaluatedResult.setPurchasePathLength(purchasePathLen);
            log.info("Evaluated result: " + JSONObject.toJSONString(evaluatedResult));
            result = Optional.of(evaluatedResult);
        }
        return result;
    }
}
