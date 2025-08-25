package com.griddb.coastal;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class CoastalMonitorApplication {

    public static void main(String[] args) {
        SpringApplication.run(CoastalMonitorApplication.class, args);
        System.out.println("Coastal Monitor Application Started!");
        System.out.println("Dashboard: http://localhost:8080/coastal-monitor");
        System.out.println("⚡ GridDB Time-Series Demo: Watching Coastlines Vanish");
    }
}