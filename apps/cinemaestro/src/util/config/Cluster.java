package util.config;

public class Cluster {
   
    private final String name;
    private final int size;
    private final String location;
    private final Component component;
    
    /**
     * Construct a new Cluster
     * 
     * @param name
     * @param size
     * @param location
     * @param component
     */
    public Cluster(final String name, final int size, final String location,
            final Component component) {
        
        this.name = name;
        this.size = size;
        this.location = location;
        this.component = component;
    }
}
