package processors;

import java.io.Serializable;

public class Statistics implements Serializable {

    private static final long serialVersionUID = -7843697432210447553L;

    private final int number;
    private final String name; 
    private final String type; 
    
    private final long start; 
    private long current = -1;     
    private long end = -1; 
    
    private long images = 0;
    private long totalImages = -1;

    private long time = -1;

    private long sizeIn = 0;
    private long sizeOut = 0;
    
    public Statistics(int number, String name, String type, long start, long totalImages) {
        this.number = number;
        this.name = name;
        this.type = type;
        this.start = start;
        this.totalImages = totalImages;
    }
    
    public synchronized void add(long current, long time, long sizeIn, long sizeOut) {
        
        this.current = current;
        this.time += time;
        this.sizeIn += sizeIn;
        this.sizeOut += sizeOut;
        images++;
    }
    
    public int getNumber() { 
        return number;
    }
    
    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public void done(long end) {
        this.end = end;
    }

    public boolean isDone() { 
        return (end != -1);
    }
    
    public long getTotalTime() {
        
        if (end != -1) { 
            return (end-start);
        } else if (current != -1) { 
            return (current-start);
        } else { 
            return 0;
        }
    }

    public long getProcessingTime() {
        
        if (time != -1) { 
            return time;
        } else { 
            return 0;
        }        
    }

    public long getIdleTime() { 
        return (getTotalTime() - getProcessingTime()); 
    }
        
    public long idlePercentage() { 
        
        long idle = getIdleTime();        
        long total = getTotalTime();
        
        if (total == 0) { 
            return 0;
        } else { 
            return (100*idle) / total;
        }
    }
    
    public long imagesPercentage() { 
        
        if (totalImages == 0) { 
            return 0;
        } else { 
            return (100*images) / totalImages;
        }
    }
    public long getImages() {
        return images;
    }

    public long getSizeIn() {
        return sizeIn;
    }

    public long getSizeOut() {
        return sizeOut;
    }
        
    public long averageImageTime() { 

        if (images == 0) { 
            return 0;
        } else { 
            return getProcessingTime() / images; 
        }       
    }

    public long averageIn() { 

        if (images == 0) { 
            return 0;
        } else { 
            return sizeIn / images; 
        }       
    }
    
    public long averageOut() { 

        if (images == 0) { 
            return 0;
        } else { 
            return sizeOut / images; 
        }       
    }
    
    private String sizeToHR(long size) { 
        
        String unit = null;
        double result = -1.0;
        
        if (size > 1024L*1024L*1024L) { 
            unit = "GB";
            result = size / (1024.0*1024.0*1024.0);
        } else if (size > 1024L*1024L) {
            unit = "MB";
            result = size / (1024.0*1024.0);
        } else if (size > 1024L) { 
            unit = "KB";
            result = size / 1024.0; 
        } else { 
            unit = "B";
            result = size; 
        }
        
        return String.format("%.2f " + unit, result);
    }
    
    private String rateToHR(long size) { 
        
        String unit = null;
        double result = -1.0;
        
        if (size > 1000L*1000L*1000L) { 
            unit = "GBit";
            result = size / (1000.0*1000.0*1000.0);
        
        } else if (size > 1000L*1000L) {
            unit = "MBit";
            result = size / (1000.0*1000.0);
        } else if (size > 1000L) { 
            unit = "KBit";
            result = size / 1000.0; 
        } else { 
            unit = "Bit";
            result = size; 
        }
        
        return String.format("%.2f " + unit, result);
    }
    
    public synchronized void printStatistics() { 
        
        long total = getTotalTime(); 
        
        if (images > 0) { 
            
            System.out.println("[*] Statistic for " + number + ": "+ name + " (" + type + ")\n"
                + "[*] Processed " + images + (totalImages > 0 ? (" of " + totalImages + " (" + imagesPercentage() + "%)" ) : "") + " images\n" 
                + "[*]  - Total time      : " + getTotalTime() + " ms.\n"
                + "[*]  - Processing time : " + getProcessingTime() + " ms.\n"
                + "[*]  - Idle time       : " + getIdleTime() + " ms. (" + idlePercentage() + "%)\n"
                + "[*]  - Avg. proc. time : " + averageImageTime() + " ms/image.\n" 
                + "[*]\n"
                + "[*]  - Data in         : " + sizeToHR(sizeIn) + "\n" 
                + "[*]  - Data out        : " + sizeToHR(sizeOut) + "\n" 
                + "[*]\n"
                + "[*] - Avg. size in    : " + sizeToHR(averageIn()) + "/image.\n"
                + "[*] - Avg. size out   : " + sizeToHR(averageOut()) + "/image.");
            
            if (total > 0) { 
                System.out.println(
                        "[*]  - Overall rate in : " + rateToHR((sizeIn*8*1000) / total) + "/sec.\n"
                      + "[*]  - Overall rate out: " + rateToHR((sizeOut*8*1000)/ total) + "/sec.");
            }
            
            if (time > 0) { 
                System.out.println(
                        "[*]  - Proc. rate in   : " + rateToHR((sizeIn*8*1000) / time) + "/sec.\n"
                      + "[*]  - Proc. rate out  : " + rateToHR((sizeOut*8*1000)/ time) + "/sec.");
            }
            
            System.out.println("[*]  - Done : " + isDone());
            
        } else { 
            System.out.println("[*] No images processed yet!");
        }
    }

    public long getTotalImages() {
        return totalImages;
    }

    public void setTotalImages(long totalImages) {
        this.totalImages = totalImages;
    }
}
