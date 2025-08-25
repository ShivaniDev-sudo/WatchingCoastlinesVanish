package com.griddb.coastal.service;

import com.griddb.coastal.model.WaterLevelData;
import com.griddb.coastal.model.MonthlyMeanData;
import com.griddb.coastal.model.StationMetadata;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

@Service
public class NOAADataService {
    
    private static final Logger logger = LoggerFactory.getLogger(NOAADataService.class);
    
    @Value("${noaa.api.base.url}")
    private String noaaBaseUrl;
    
    @Value("${noaa.api.metadata.url}")
    private String noaaMetadataUrl;
    
    @Value("${noaa.api.application}")
    private String applicationName;
    
    @Value("${noaa.stations}")
    private String stationsConfig;
    
    private Map<String, StationMetadata> stationCache = new HashMap<>();
    
    /**
     * Fetch latest water level data for a station
     */
    public List<WaterLevelData> fetchLatestWaterLevels(String stationId) {
        List<WaterLevelData> waterLevels = new ArrayList<>();
        
        try {
            String urlStr = String.format("%s?product=water_level&application=%s&station=%s&date=latest&datum=MLLW&time_zone=gmt&units=metric&format=json",
                    noaaBaseUrl, applicationName, stationId);
            
            logger.info("Fetching latest water levels from: {}", urlStr);
            
            String response = makeHttpRequest(urlStr);
            JSONObject jsonResponse = new JSONObject(response);
            
            if (jsonResponse.has("data")) {
                JSONArray dataArray = jsonResponse.getJSONArray("data");
                StationMetadata station = getOrFetchStationMetadata(stationId);
                
                for (int i = 0; i < dataArray.length(); i++) {
                    JSONObject dataPoint = dataArray.getJSONObject(i);
                    
                    // NOAA format: {"t": "yyyy-MM-dd HH:mm", "v": "x.xxx", "f": "..."}
                    String timestamp = dataPoint.getString("t");
                    double waterLevel = Double.parseDouble(dataPoint.getString("v"));
                    String flags = dataPoint.optString("f", "");
                    
                    LocalDateTime dateTime = LocalDateTime.parse(timestamp, 
                        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm"));
                    
                    WaterLevelData wld = new WaterLevelData(
                        stationId, 
                        station.getStationName(),
                        dateTime,
                        waterLevel,
                        "MLLW",
                        station.getLatitude(),
                        station.getLongitude(),
                        flags
                    );
                    waterLevels.add(wld);
                }
            }
            
            logger.info("Fetched {} water level readings for station {}", waterLevels.size(), stationId);
            
        } catch (Exception e) {
            logger.error("Error fetching water level data for station {}: {}", stationId, e.getMessage());
        }
        
        return waterLevels;
    }
    
 
    public List<WaterLevelData> fetchRecentWaterLevels(String stationId, int days) {
        List<WaterLevelData> waterLevels = new ArrayList<>();
        
        try {
            LocalDate endDate = LocalDate.now();
            LocalDate startDate = endDate.minusDays(days);
            
            String urlStr = String.format("%s?product=water_level&application=%s&station=%s&begin_date=%s&end_date=%s&datum=MLLW&time_zone=gmt&units=metric&format=json",
                    noaaBaseUrl, applicationName, stationId, 
                    startDate.format(DateTimeFormatter.ofPattern("yyyyMMdd")),
                    endDate.format(DateTimeFormatter.ofPattern("yyyyMMdd")));
            
            logger.info("Fetching recent water levels from: {}", urlStr);
            
            String response = makeHttpRequest(urlStr);
            JSONObject jsonResponse = new JSONObject(response);
            
            if (jsonResponse.has("data")) {
                JSONArray dataArray = jsonResponse.getJSONArray("data");
                StationMetadata station = getOrFetchStationMetadata(stationId);
                
                for (int i = 0; i < dataArray.length(); i++) {
                    JSONObject dataPoint = dataArray.getJSONObject(i);
                    
                    String timestamp = dataPoint.getString("t");
                    double waterLevel = Double.parseDouble(dataPoint.getString("v"));
                    String flags = dataPoint.optString("f", "");
                    
                    LocalDateTime dateTime = LocalDateTime.parse(timestamp, 
                        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm"));
                    
                    WaterLevelData wld = new WaterLevelData(
                        stationId, 
                        station.getStationName(),
                        dateTime,
                        waterLevel,
                        "MLLW",
                        station.getLatitude(),
                        station.getLongitude(),
                        flags
                    );
                    waterLevels.add(wld);
                }
            }
            
            logger.info("Fetched {} historical water level readings for station {}", waterLevels.size(), stationId);
            
        } catch (Exception e) {
            logger.error("Error fetching recent water level data for station {}: {}", stationId, e.getMessage());
        }
        
        return waterLevels;
    }
    
    /**
     * Fetch monthly mean data for long-term trend analysis
     */
    public List<MonthlyMeanData> fetchMonthlyMeanData(String stationId, int years) {
        List<MonthlyMeanData> monthlyData = new ArrayList<>();
        
        try {
            LocalDate endDate = LocalDate.now();
            LocalDate startDate = endDate.minusYears(years);
            
            String urlStr = String.format("%s?product=monthly_mean&application=%s&station=%s&begin_date=%s&end_date=%s&datum=MSL&time_zone=gmt&units=metric&format=json",
                    noaaBaseUrl, applicationName, stationId, 
                    startDate.format(DateTimeFormatter.ofPattern("yyyyMMdd")),
                    endDate.format(DateTimeFormatter.ofPattern("yyyyMMdd")));
            
            logger.info("Fetching monthly mean data from: {}", urlStr);
            
            String response = makeHttpRequest(urlStr);
            JSONObject jsonResponse = new JSONObject(response);
            
            if (jsonResponse.has("data")) {
                JSONArray dataArray = jsonResponse.getJSONArray("data");
                StationMetadata station = getOrFetchStationMetadata(stationId);
                
                for (int i = 0; i < dataArray.length(); i++) {
                    JSONObject dataPoint = dataArray.getJSONObject(i);
                    
                    // NOAA format: {"year": "YYYY", "month": "MM", "MSL": "x.xxx"}
                    int year = Integer.parseInt(dataPoint.getString("year"));
                    int monthNumber = Integer.parseInt(dataPoint.getString("month"));
                    double meanLevel = Double.parseDouble(dataPoint.getString("MSL"));

                    LocalDate month = LocalDate.of(year, monthNumber, 1);
                    
                    MonthlyMeanData mmd = new MonthlyMeanData(
                        stationId,
                        station.getStationName(),
                        month,
                        meanLevel,
                        year,
                        monthNumber,
                        station.getLatitude(),
                        station.getLongitude()
                    );
                    monthlyData.add(mmd);
                }
            }
            
            logger.info("Fetched {} monthly mean readings for station {}", monthlyData.size(), stationId);
            
        } catch (Exception e) {
            logger.error("Error fetching monthly mean data for station {}: {}", stationId, e.getMessage());
        }
        
        return monthlyData;
    }
    
    /**
     * Get configured monitoring stations
     */
    public List<StationMetadata> getConfiguredStations() {
        List<StationMetadata> stations = new ArrayList<>();
        
        String[] stationConfigs = stationsConfig.split(";");
        for (String config : stationConfigs) {
            String[] parts = config.split(",");
            if (parts.length >= 6) {
                StationMetadata station = StationMetadata.builder()
                    .stationId(parts[0].trim())
                    .stationName(parts[1].trim())
                    .state(parts[2].trim())
                    .latitude(Double.parseDouble(parts[3].trim()))
                    .longitude(Double.parseDouble(parts[4].trim()))
                    .isActive(Boolean.parseBoolean(parts[5].trim()))
                    .build();
                stations.add(station);
            }
        }
        
        return stations;
    }
    
    /**
     * Get or fetch station metadata
     */
    private StationMetadata getOrFetchStationMetadata(String stationId) {
        if (stationCache.containsKey(stationId)) {
            return stationCache.get(stationId);
        }
        
        // Return default station info if not in cache
        StationMetadata station = StationMetadata.builder()
            .stationId(stationId)
            .stationName("Station " + stationId)
            .latitude(40.0) // Default coordinates
            .longitude(-74.0)
            .build();
        stationCache.put(stationId, station);
        
        return station;
    }
    
    /**
     * Make HTTP request to NOAA API
     */
    private String makeHttpRequest(String urlStr) throws Exception {
        URL url = new URL(urlStr);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        conn.setRequestProperty("User-Agent", "GridDB-CoastalMonitor/1.0");
        
        int responseCode = conn.getResponseCode();
        if (responseCode != 200) {
            throw new RuntimeException("HTTP Error: " + responseCode);
        }
        
        BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));
        StringBuilder response = new StringBuilder();
        String line;
        
        while ((line = reader.readLine()) != null) {
            response.append(line);
        }
        reader.close();
        
        String responseStr = response.toString();
        logger.info("NOAA API Response for {}: {}", urlStr, responseStr);
        if (responseStr.trim().isEmpty()) {
            return "{}";
        }
        return responseStr;
    }
}