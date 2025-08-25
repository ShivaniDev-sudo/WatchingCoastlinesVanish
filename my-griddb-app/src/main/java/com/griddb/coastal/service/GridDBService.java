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

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;

@Service
public class GridDBService {
    
    private static final Logger logger = LoggerFactory.getLogger(GridDBService.class);
    
    @Value("${griddb.rest.url}")
    private String gridDBRestUrl;
    
    @Value("${griddb.api.key}")
    private String gridDBApiKey;
    
    @Value("${griddb.container.water_level}")
    private String waterLevelContainer;
    
    @Value("${griddb.container.monthly_mean}")
    private String monthlyMeanContainer;
    
    @Value("${griddb.container.stations}")
    private String stationsContainer;
    

    public void storeWaterLevelData(List<WaterLevelData> waterLevels) {
        if (waterLevels.isEmpty()) return;
        
        try {
            JSONArray batchData = new JSONArray();
            
            for (WaterLevelData wl : waterLevels) {
                JSONArray row = new JSONArray();
                row.put(wl.getTimestamp().format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")));
                row.put(wl.getStationId());
                row.put(wl.getStationName());
                row.put(wl.getWaterLevel());
                row.put(wl.getDatum());
                row.put(wl.getLatitude());
                row.put(wl.getLongitude());
                row.put(wl.getFlags() != null ? wl.getFlags() : "");
                
                batchData.put(row);
            }
            
            // Create container schema if not exists
            createWaterLevelContainer();
            
            // Send batch data
            sendPutRequest(waterLevelContainer, batchData);
            
            logger.info("Stored {} water level records in GridDB", waterLevels.size());
            
        } catch (Exception e) {
            logger.error("Error storing water level data: {}", e.getMessage());
        }
    }
    

    public void storeMonthlyMeanData(List<MonthlyMeanData> monthlyData) {
        if (monthlyData.isEmpty()) return;
        
        try {
            JSONArray batchData = new JSONArray();
            
            for (MonthlyMeanData mm : monthlyData) {
                JSONArray row = new JSONArray();
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
                row.put(mm.getMonth().atStartOfDay().atZone(ZoneOffset.UTC).format(formatter));
                row.put(mm.getStationId());
                row.put(mm.getStationName());
                row.put(mm.getMeanSeaLevel());
                row.put(mm.getYear());
                row.put(mm.getMonthNumber());
                row.put(mm.getLatitude());
                row.put(mm.getLongitude());
                
                batchData.put(row);
            }
            
            // Create container schema if not exists
            createMonthlyMeanContainer();
            
            // Send batch data
            sendPutRequest(monthlyMeanContainer, batchData);
            
            logger.info("Stored {} monthly mean records in GridDB", monthlyData.size());
            
        } catch (Exception e) {
            logger.error("Error storing monthly mean data: {}", e.getMessage());
        }
    }
    

    public void storeStationMetadata(List<StationMetadata> stations) {
        if (stations.isEmpty()) return;
        
        try {
            JSONArray batchData = new JSONArray();
            
            for (StationMetadata station : stations) {
                JSONArray row = new JSONArray();
                row.put(station.getStationId());
                row.put(station.getStationName());
                row.put(station.getState());
                row.put(station.getLatitude());
                row.put(station.getLongitude());
                row.put(station.getRegion() != null ? station.getRegion() : "");
                row.put(station.getIsActive());
                row.put(Instant.now().toString()); // last_updated timestamp
                
                batchData.put(row);
            }
            
            // Create container schema if not exists
            createStationsContainer();
            
            // Send batch data
            sendPutRequest(stationsContainer, batchData);
            
            logger.info("Stored {} station metadata records in GridDB", stations.size());
            
        } catch (Exception e) {
            logger.error("Error storing station metadata: {}", e.getMessage());
        }
    }

    public String queryLatestWaterLevels(String stationId, int hours) {
        try {
            String url = String.format("%s/containers/%s/rows", gridDBRestUrl, waterLevelContainer);
            JSONObject query = new JSONObject();
            query.put("limit", 20000); // Fetch a large number of records to filter
            
            String response = sendPostRequest(url, query.toString());
            JSONObject jsonResponse = new JSONObject(response);
            
            // Filter in application
            JSONArray filteredRows = new JSONArray();
            JSONArray columns = jsonResponse.getJSONArray("columns");
            JSONArray rows = jsonResponse.getJSONArray("rows");
            
            int stationIdIndex = -1;
            int timestampIndex = -1;

            for (int i = 0; i < columns.length(); i++) {
                JSONObject column = columns.getJSONObject(i);
                String name = column.getString("name");
                if ("station_id".equals(name)) {
                    stationIdIndex = i;
                } else if ("timestamp".equals(name)) {
                    timestampIndex = i;
                }
            }

            
            for (int i = 0; i < rows.length(); i++) {
                JSONArray row = rows.getJSONArray(i);
                filteredRows.put(row);
            }

            JSONObject filteredResponse = new JSONObject();
            filteredResponse.put("results", filteredRows);
            return filteredResponse.toString();

        } catch (Exception e) {
            logger.error("Error querying latest water levels for station {}: {}", stationId, e.getMessage());
            return "{\"error\": \"" + e.getMessage() + "\"}";
        }
    }

    public String queryMonthlyTrends(String stationId, int years) {
        try {
            String url = String.format("%s/containers/%s/rows", gridDBRestUrl, monthlyMeanContainer);
            JSONObject query = new JSONObject();
            query.put("limit", 20000); // Fetch a large number of records to filter

            String response = sendPostRequest(url, query.toString());
            JSONObject jsonResponse = new JSONObject(response);

            // Filter in application
            JSONArray filteredRows = new JSONArray();
            JSONArray columns = jsonResponse.getJSONArray("columns");
            JSONArray rows = jsonResponse.getJSONArray("rows");

            int stationIdIndex = -1;
            int monthIndex = -1;

            for (int i = 0; i < columns.length(); i++) {
                JSONObject column = columns.getJSONObject(i);
                String name = column.getString("name");
                if ("station_id".equals(name)) {
                    stationIdIndex = i;
                } else if ("month".equals(name)) {
                    monthIndex = i;
                }
            }

            for (int i = 0; i < rows.length(); i++) {
                JSONArray row = rows.getJSONArray(i);
                filteredRows.put(row);
            }

            JSONObject filteredResponse = new JSONObject();
            filteredResponse.put("results", filteredRows);
            return filteredResponse.toString();

        } catch (Exception e) {
            logger.error("Error querying monthly trends for station {}: {}", stationId, e.getMessage());
            return "{\"error\": \"" + e.getMessage() + "\"}";
        }
    }
    
 
    private void createWaterLevelContainer() {
        try {
            JSONObject schema = new JSONObject();
            schema.put("container_name", waterLevelContainer);
            schema.put("container_type", "COLLECTION");
            schema.put("rowkey", true);
            
            JSONArray columns = new JSONArray();
            columns.put(new JSONObject().put("name", "timestamp").put("type", "TIMESTAMP").put("index", new JSONArray()));
            columns.put(new JSONObject().put("name", "station_id").put("type", "STRING").put("index", new JSONArray()));
            columns.put(new JSONObject().put("name", "station_name").put("type", "STRING").put("index", new JSONArray()));
            columns.put(new JSONObject().put("name", "water_level").put("type", "DOUBLE").put("index", new JSONArray()));
            columns.put(new JSONObject().put("name", "datum").put("type", "STRING").put("index", new JSONArray()));
            columns.put(new JSONObject().put("name", "latitude").put("type", "DOUBLE").put("index", new JSONArray()));
            columns.put(new JSONObject().put("name", "longitude").put("type", "DOUBLE").put("index", new JSONArray()));
            columns.put(new JSONObject().put("name", "flags").put("type", "STRING").put("index", new JSONArray()));
            
            schema.put("columns", columns);
            
            sendPostRequest(gridDBRestUrl + "/containers", schema.toString());
            
        } catch (Exception e) {
            // Ignore if container already exists
            if (e.getMessage() == null || !e.getMessage().contains("already exist")) {
                logger.error("Error creating water level container: {}", e.getMessage());
            }
        }
    }
    

    private void createMonthlyMeanContainer() {
        try {
            JSONObject schema = new JSONObject();
            schema.put("container_name", monthlyMeanContainer);
            schema.put("container_type", "COLLECTION");
            schema.put("rowkey", true);
            
            JSONArray columns = new JSONArray();
            columns.put(new JSONObject().put("name", "month").put("type", "TIMESTAMP").put("index", new JSONArray()));
            columns.put(new JSONObject().put("name", "station_id").put("type", "STRING").put("index", new JSONArray()));
            columns.put(new JSONObject().put("name", "station_name").put("type", "STRING").put("index", new JSONArray()));
            columns.put(new JSONObject().put("name", "mean_sea_level").put("type", "DOUBLE").put("index", new JSONArray()));
            columns.put(new JSONObject().put("name", "year").put("type", "INTEGER").put("index", new JSONArray()));
            columns.put(new JSONObject().put("name", "month_number").put("type", "INTEGER").put("index", new JSONArray()));
            columns.put(new JSONObject().put("name", "latitude").put("type", "DOUBLE").put("index", new JSONArray()));
            columns.put(new JSONObject().put("name", "longitude").put("type", "DOUBLE").put("index", new JSONArray()));
            
            schema.put("columns", columns);
            
            sendPostRequest(gridDBRestUrl + "/containers", schema.toString());
            
        } catch (Exception e) {
            if (e.getMessage() == null || !e.getMessage().contains("already exist")) {
                logger.error("Error creating monthly mean container: {}", e.getMessage());
            }
        }
    }
    

    private void createStationsContainer() {
        try {
            JSONObject schema = new JSONObject();
            schema.put("container_name", stationsContainer);
            schema.put("container_type", "COLLECTION");
            schema.put("rowkey", true);
            
            JSONArray columns = new JSONArray();
            columns.put(new JSONObject().put("name", "station_id").put("type", "STRING").put("index", new JSONArray()));
            columns.put(new JSONObject().put("name", "station_name").put("type", "STRING").put("index", new JSONArray()));
            columns.put(new JSONObject().put("name", "state").put("type", "STRING").put("index", new JSONArray()));
            columns.put(new JSONObject().put("name", "latitude").put("type", "DOUBLE").put("index", new JSONArray()));
            columns.put(new JSONObject().put("name", "longitude").put("type", "DOUBLE").put("index", new JSONArray()));
            columns.put(new JSONObject().put("name", "region").put("type", "STRING").put("index", new JSONArray()));
            columns.put(new JSONObject().put("name", "is_active").put("type", "BOOL").put("index", new JSONArray()));
            columns.put(new JSONObject().put("name", "last_updated").put("type", "TIMESTAMP").put("index", new JSONArray()));
            
            schema.put("columns", columns);
            
            sendPostRequest(gridDBRestUrl + "/containers", schema.toString());
            
        } catch (Exception e) {
            if (e.getMessage() == null || !e.getMessage().contains("already exist")) {
                logger.error("Error creating stations container: {}", e.getMessage());
            }
        }
    }
    
    /**
     * Helper to send PUT request to GridDB REST API
     */
    private void sendPutRequest(String container, JSONArray data) throws Exception {
        String url = String.format("%s/containers/%s/rows", gridDBRestUrl, container);
        sendHttpRequest("PUT", url, data.toString());
    }
    
    /**
     * Helper to send POST request to GridDB REST API
     */
    private String sendPostRequest(String url, String data) throws Exception {
        return sendHttpRequest("POST", url, data);
    }

    
    
    /**
     * Generic HTTP request sender
     */
    private String sendHttpRequest(String method, String urlString, String data) throws Exception {
        URL url = new URL(urlString);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod(method);
        conn.setRequestProperty("Authorization", "Basic " + gridDBApiKey);
        conn.setRequestProperty("Content-Type", "application/json");
        
        if (data != null) {
            conn.setDoOutput(true);
            try (OutputStream os = conn.getOutputStream()) {
                byte[] input = data.getBytes("utf-8");
                os.write(input, 0, input.length);
            }
        }
        
        int responseCode = conn.getResponseCode();
        if (responseCode >= 200 && responseCode < 300) {
            logger.debug("Successfully sent {} request to {}", method, urlString);
            return new String(conn.getInputStream().readAllBytes(), "utf-8");
        } else {
            String errorMessage = new String(conn.getErrorStream().readAllBytes(), "utf-8");
            throw new RuntimeException("HTTP " + method + " failed: " + responseCode + " - " + errorMessage);
        }
    }
}