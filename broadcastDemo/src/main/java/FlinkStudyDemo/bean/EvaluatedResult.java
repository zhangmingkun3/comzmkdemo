package FlinkStudyDemo.bean;

import java.util.Map;

public class EvaluatedResult {
    private String userId;
    private String channel;
    private int purchasePathLength;
    private Map<String, Integer> eventTypeCounts;


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

    public int getPurchasePathLength() {
        return purchasePathLength;
    }

    public void setPurchasePathLength(int purchasePathLength) {
        this.purchasePathLength = purchasePathLength;
    }

    public Map<String, Integer> getEventTypeCounts() {
        return eventTypeCounts;
    }

    public void setEventTypeCounts(Map<String, Integer> eventTypeCounts) {
        this.eventTypeCounts = eventTypeCounts;
    }
}
