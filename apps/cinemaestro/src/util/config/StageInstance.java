package util.config;

class StageInstance {

    private final Stage instance;
    
    private final int getQueueLength;
    private final int putQueueLength;
    
    public StageInstance(Stage instance, int getQueueLength, int putQueueLength) {
        this.instance = instance;
        this.getQueueLength = getQueueLength;
        this.putQueueLength = putQueueLength;
    }

    public Stage getInstance() { 
        return instance;
    }
    
    public int getGetQueueLength() {
        return getQueueLength;
    }

    public int getPutQueueLength() {
        return putQueueLength;
    }

}
