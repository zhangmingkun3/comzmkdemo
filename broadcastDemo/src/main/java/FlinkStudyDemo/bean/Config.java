package FlinkStudyDemo.bean;

import com.alibaba.fastjson.JSONObject;

import java.util.Map;

public class Config {

    private String channel;
    private String registerDate;
    private int historyPurchaseTimes;
    private int maxPurchasePathLength;


    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public String getRegisterDate() {
        return registerDate;
    }

    public void setRegisterDate(String registerDate) {
        this.registerDate = registerDate;
    }

    public int getHistoryPurchaseTimes() {
        return historyPurchaseTimes;
    }

    public void setHistoryPurchaseTimes(int historyPurchaseTimes) {
        this.historyPurchaseTimes = historyPurchaseTimes;
    }

    public int getMaxPurchasePathLength() {
        return maxPurchasePathLength;
    }

    public void setMaxPurchasePathLength(int maxPurchasePathLength) {
        this.maxPurchasePathLength = maxPurchasePathLength;
    }

    public static Config buildConfig(String s){
        JSONObject jsonObject = JSONObject.parseObject(s);
        Map<Object, Object> map = (Map)jsonObject;
        Config config = new Config();
        config.setChannel(map.get("channel").toString());
        config.setRegisterDate(map.get("registerDate").toString());
        config.setHistoryPurchaseTimes(Integer.getInteger(map.get("historyPurchaseTimes").toString()));
        config.setMaxPurchasePathLength(Integer.getInteger(map.get("maxPurchasePathLength").toString()));
        return config;
    }
}
