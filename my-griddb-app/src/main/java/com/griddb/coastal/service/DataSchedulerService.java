package com.griddb.coastal.service;

import com.griddb.coastal.model.WaterLevelData;
import com.griddb.coastal.model.MonthlyMeanData;
import com.griddb.coastal.model.StationMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.util.List;

@Service
public class DataSchedulerService {
    
    private static final Logger logger = LoggerFactory.getLogger(DataSchedulerService.class);
    
    @Autowired
    private NOAADataService noaaDataService;
    
    @Autowired
    private GridDBService gridDBService;
    
 
    @PostConstruct
    public void initializeData() {
        logger.info("🚀 Initializing Coastal Monitor Data Collection...");
        
        // Load and store station metadata
        List<StationMetadata> stations = noaaDataService.getConfiguredStations();
     gridDBService.storeStationMetadata(stations);
        
        // Load initial historical data (last 7 days) for each station
         for (StationMetadata station : stations) {
            try {
                logger.info("Loading initial data for station: {}", station.getStationName());
                
                // Load recent water levels
                List<WaterLevelData> recentData = noaaDataService.fetchRecentWaterLevels(
                    station.getStationId(), 7);
                if (!recentData.isEmpty()) {
                    gridDBService.storeWaterLevelData(recentData);
                }
                
                // Load monthly trends (last 5 years)
                List<MonthlyMeanData> monthlyData = noaaDataService.fetchMonthlyMeanData(
                    station.getStationId(), 5);
                if (!monthlyData.isEmpty()) {
                    gridDBService.storeMonthlyMeanData(monthlyData);
                }
                
                // Small delay to avoid overwhelming NOAA API
                Thread.sleep(2000);
                
            } catch (Exception e) {
                logger.error("Error initializing data for station {}: {}", 
                    station.getStationId(), e.getMessage());
            }
        }
        
        logger.info("✅ Initial data loading completed!");
    }
    
  
    @Scheduled(cron = "${scheduler.water.level.cron}")
    public void fetchLatestWaterLevels() {
        logger.info("🌊 Scheduled task: Fetching latest water levels...");
        
        List<StationMetadata> stations = noaaDataService.getConfiguredStations();
        int totalRecords = 0;
        
        for (StationMetadata station : stations) {
            try {
                List<WaterLevelData> latestData = noaaDataService.fetchLatestWaterLevels(
                    station.getStationId());
                
                if (!latestData.isEmpty()) {
                    gridDBService.storeWaterLevelData(latestData);
                    totalRecords += latestData.size();
                    logger.debug("Updated {} records for {}", latestData.size(), station.getStationName());
                }
                
                // Rate limiting
                Thread.sleep(1000);
                
            } catch (Exception e) {
                logger.error("Error fetching latest data for station {}: {}", 
                    station.getStationId(), e.getMessage());
            }
        }
        
        logger.info("✅ Collected {} new water level records", totalRecords);
    }
    
    /**
     * Scheduled task to update monthly mean data daily at 2 AM
     */
    @Scheduled(cron = "${scheduler.monthly.mean.cron}")
    public void updateMonthlyMeanData() {
        logger.info("📊 Scheduled task: Updating monthly mean data...");
        
        List<StationMetadata> stations = noaaDataService.getConfiguredStations();
        int totalRecords = 0;
        
        for (StationMetadata station : stations) {
            try {
                // Fetch last 6 months of monthly data to catch any updates
                List<MonthlyMeanData> monthlyData = noaaDataService.fetchMonthlyMeanData(
                    station.getStationId(), 1); // 1 year to ensure we get recent months
                
                if (!monthlyData.isEmpty()) {
                    gridDBService.storeMonthlyMeanData(monthlyData);
                    totalRecords += monthlyData.size();
                    logger.debug("Updated {} monthly records for {}", 
                        monthlyData.size(), station.getStationName());
                }
                
                // Rate limiting
                Thread.sleep(2000);
                
            } catch (Exception e) {
                logger.error("Error updating monthly data for station {}: {}", 
                    station.getStationId(), e.getMessage());
            }
        }
        
        logger.info("✅ Updated {} monthly mean records", totalRecords);
    }
    
 
    public void triggerDataCollection() {
        logger.info("🔄 Manual trigger: Collecting latest data for all stations...");
        fetchLatestWaterLevels();
    }
    

    public  void triggerMonthlyUpdate() {
        logger.info("🔄 Manual trigger: Updating monthly data for all stations...");
        updateMonthlyMeanData();
    }
}