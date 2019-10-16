package FlinkStudyDemo.bean;


import com.alibaba.fastjson.JSONObject;

import java.util.Map;

public class UserEvent {

    private String userId;
    private String channel;
    private String eventType;
    private Long eventTimestamp;
    private String data;

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public Long getEventTimestamp() {
        return eventTimestamp;
    }

    public void setEventTimestamp(Long eventTimestamp) {
        this.eventTimestamp = eventTimestamp;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    public static UserEvent buildEvent(String s){
        JSONObject jsonObject = JSONObject.parseObject(s);
        Map<Object, Object> map = (Map)jsonObject;
        UserEvent userEvent = new UserEvent();
        userEvent.setChannel(map.get("channel").toString());
        userEvent.setUserId(map.get("userId").toString());
        userEvent.setEventType(map.get("eventType").toString());
        userEvent.setEventTimestamp(Long.getLong(map.get("eventTime").toString()));
        userEvent.setData(map.get("data").toString());
        return userEvent;
    }
}
