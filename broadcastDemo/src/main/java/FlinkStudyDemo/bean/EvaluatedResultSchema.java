package FlinkStudyDemo.bean;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class EvaluatedResultSchema implements SerializationSchema<EvaluatedResult> {

    private EvaluatedResult evaluatedResult;

    public EvaluatedResultSchema() {
    }

    public EvaluatedResultSchema(EvaluatedResult evaluatedResult) {
        this.evaluatedResult = evaluatedResult;
    }

//    @Override
//    public EvaluatedResult deserialize(byte[] bytes, byte[] bytes1, String s, int i, long l) throws IOException {
//        String value = null;
//        if (bytes != null) {
//            value = new String(bytes, StandardCharsets.UTF_8);
//        }
//        JSONObject jsonObject = JSONObject.parseObject(value);
//        Map<Object, Object> map = (Map)jsonObject;
//        evaluatedResult.setChannel(map.get("channel").toString());
//        evaluatedResult.setEventTypeCounts(map.get("eventTypeCounts").toString());
//        evaluatedResult.setPurchasePathLength(Integer.getInteger(map.get("purchasePathLength").toString()));
//        evaluatedResult.setUserId(map.get("userId").toString());
//        return evaluatedResult;
//    }
//
//    @Override
//    public boolean isEndOfStream(EvaluatedResult evaluatedResult) {
//        return false;
//    }
//
//    @Override
//    public TypeInformation<EvaluatedResult> getProducedType() {
//        return null;
//    }

    @Override
    public byte[] serialize(EvaluatedResult evaluatedResult) {
        return new byte[0];
    }
}
