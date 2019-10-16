package FlinkStudyDemo.bean;

import java.util.List;

public class UserEventContainer {

    private String userId;
    private List<UserEvent> userEvents;

    public List<UserEvent> getUserEvents() {
        return userEvents;
    }

    public void setUserEvents(List<UserEvent> userEvents) {
        this.userEvents = userEvents;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }
}
