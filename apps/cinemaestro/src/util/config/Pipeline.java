package util.config;

import java.util.LinkedList;

class Pipeline {
    
    private final String name;
    private final LinkedList<Stage> stages = new LinkedList<Stage>();
    
    public Pipeline(final String name) {
        this.name = name;
    }
    
    public String getName() { 
        return name;
    }
    
    public void addStage(Stage stage) {
        stages.add(stage);
    }
    
    public Stage [] getStages() {
        return stages.toArray(new Stage[stages.size()]);
    }
    
    public int getLength() {
        return stages.size();
    }
    
    public int getMaxWidth() { 
        
        int w = 0;
        
        for (Stage s : stages) { 
            
            int tmp = s.getWidth();
            
            if (tmp > w) { 
                w = tmp;
            }
        }

        return w;
    }  
}
