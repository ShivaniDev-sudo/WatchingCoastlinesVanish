package com.griddb.coastal.controller;

import com.griddb.coastal.service.NOAADataService;
import com.griddb.coastal.service.GridDBService;
import com.griddb.coastal.service.DataSchedulerService;
import com.griddb.coastal.model.StationMetadata;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@Controller
public class DashboardController {
    
    private static final Logger logger = LoggerFactory.getLogger(DashboardController.class);
    
    @Autowired
    private NOAADataService noaaDataService;
    
    @Autowired
    private GridDBService gridDBService;
    
    @Autowired
    private DataSchedulerService schedulerService;
    

    @GetMapping("/")
    public String dashboard(Model model) {
        logger.info("Rendering main dashboard");
        
        List<StationMetadata> stations = noaaDataService.getConfiguredStations();
        model.addAttribute("stations", stations);
        model.addAttribute("title", "Watching Coastlines Vanish");
        model.addAttribute("subtitle", "Real-time Coastal Monitoring with GridDB Time-Series");
        
        return "dashboard";
    }
    

    @GetMapping("/api/water-levels/{stationId}")
    @ResponseBody
    public ResponseEntity<String> getWaterLevels(@PathVariable String stationId, 
                                               @RequestParam(defaultValue = "24") int hours) {
        try {
            String data = gridDBService.queryLatestWaterLevels(stationId, hours);
            return ResponseEntity.ok(data);
        } catch (Exception e) {
            logger.error("Error fetching water levels for station {}: {}", stationId, e.getMessage());
            return ResponseEntity.internalServerError().body("{\"error\": \"" + e.getMessage() + "\"}");
        }
    }
    

    @GetMapping("/api/monthly-trends/{stationId}")
    @ResponseBody
    public ResponseEntity<String> getMonthlyTrends(@PathVariable String stationId,
                                                  @RequestParam(defaultValue = "10") int years) {
        try {
            String data = gridDBService.queryMonthlyTrends(stationId, years);
            return ResponseEntity.ok(data);
        } catch (Exception e) {
            logger.error("Error fetching monthly trends for station {}: {}", stationId, e.getMessage());
            return ResponseEntity.internalServerError().body("{\"error\": \"" + e.getMessage() + "\"}");
        }
    }
    

    @GetMapping("/api/stations")
    @ResponseBody
    public ResponseEntity<List<StationMetadata>> getAllStations() {
        try {
            List<StationMetadata> stations = noaaDataService.getConfiguredStations();
            return ResponseEntity.ok(stations);
        } catch (Exception e) {
            logger.error("Error fetching stations: {}", e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }

    @GetMapping("/api/dashboard-data")
    @ResponseBody
    public ResponseEntity<String> getDashboardData() {
        try {
            JSONObject dashboardData = new JSONObject();
            JSONArray stationsData = new JSONArray();
            
            List<StationMetadata> stations = noaaDataService.getConfiguredStations();
            
            for (StationMetadata station : stations) {
                JSONObject stationInfo = new JSONObject();
                stationInfo.put("stationId", station.getStationId());
                stationInfo.put("stationName", station.getStationName());
                stationInfo.put("state", station.getState());
                stationInfo.put("latitude", station.getLatitude());
                stationInfo.put("longitude", station.getLongitude());
                
                // Get latest water level
                String latestDataString = gridDBService.queryLatestWaterLevels(station.getStationId(), 1);
                JSONObject latestDataObject = new JSONObject(latestDataString);
                stationInfo.put("latestWaterLevel", latestDataObject);
                
                // Get monthly trend (last 2 years for quick overview)
                String trendDataString = gridDBService.queryMonthlyTrends(station.getStationId(), 2);
                JSONObject trendDataObject = new JSONObject(trendDataString);
                logger.info("Monthly Trend Data for {}: {}", station.getStationId(), trendDataObject.toString());
                stationInfo.put("monthlyTrend", trendDataObject);
                
                stationsData.put(stationInfo);
            }
            
            dashboardData.put("stations", stationsData);
            dashboardData.put("lastUpdated", System.currentTimeMillis());
            dashboardData.put("totalStations", stations.size());
            
            return ResponseEntity.ok(dashboardData.toString());
            
        } catch (Exception e) {
            logger.error("Error generating dashboard data: {}", e.getMessage());
            return ResponseEntity.internalServerError().body("{\"error\": \"" + e.getMessage() + "\"}");
        }
    }
    
    @PostMapping("/api/trigger-collection")
    @ResponseBody
    public ResponseEntity<String> triggerDataCollection() {
        try {
            schedulerService.triggerDataCollection();
            JSONObject response = new JSONObject();
            response.put("status", "success");
            response.put("message", "Data collection triggered successfully");
            response.put("timestamp", System.currentTimeMillis());
            
            return ResponseEntity.ok(response.toString());
        } catch (Exception e) {
            logger.error("Error triggering data collection: {}", e.getMessage());
            return ResponseEntity.internalServerError().body("{\"error\": \"" + e.getMessage() + "\"}");
        }
    }

    @PostMapping("/api/trigger-monthly-update")
    @ResponseBody
    public ResponseEntity<String> triggerMonthlyUpdate() {
        try {
            schedulerService.triggerMonthlyUpdate();
            JSONObject response = new JSONObject();
            response.put("status", "success");
            response.put("message", "Monthly data update triggered successfully");
            response.put("timestamp", System.currentTimeMillis());
            
            return ResponseEntity.ok(response.toString());
        } catch (Exception e) {
            logger.error("Error triggering monthly update: {}", e.getMessage());
            return ResponseEntity.internalServerError().body("{\"error\": \"" + e.getMessage() + "\"}");
        }
    }
}
