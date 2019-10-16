package FlinkStudyDemo.bean;

public enum EventType {

    VIEW_PRODUCT,
    ADD_TO_CART,
            REMOVE_FROM_CART,
    PURCHASE,
    ;

//    public static EventType valueOf(String value){
//        for (EventType eventType : EventType.values()){
//            if (eventType.name().equals(value)){
//                return eventType;
//            }
//        }
//        return null;
//    }
}
