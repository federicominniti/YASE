package it.unipi.dii.mircv.yase.util;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;

public class Util {
    /**
     * Getter of the free heap percentage
     * @return
     */
    public static double getFreeHeapPercentage(){
        MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
        MemoryUsage heapUsage = memoryBean.getHeapMemoryUsage();
        return ((double) heapUsage.getMax() - heapUsage.getUsed()) / heapUsage.getMax();
    }
}
