package util.config;

class Stage {
   
    private final Component component; 
    private final int width;
    private final int getQueueLength;
    private final int putQueueLength;
    
    public Stage(Component component, int width, int getLength, int putLength) {
        this.component = component;
        this.width = width;
        this.getQueueLength = getLength;
        this.putQueueLength = putLength;
    }

    public int getWidth() {
        return width;
    }
    
    public int getGetQueueLength() {
        return getQueueLength;
    }
    
    public int getPutQueueLength() {
        return putQueueLength;
    }
   
    /*
    public boolean hasInputQueues() { 
        return component.hasInputQueue();
    }
    
    public boolean hasOutputQueues() { 
        return component.hasOutputQueue();
    }

    public Class getInputType() { 
        return component.getInputType();
    }
    
    public Class getOutputType() { 
        return component.getOutputType();
    }*/
    
    public Component getComponent() {
        return component;
    }
}
