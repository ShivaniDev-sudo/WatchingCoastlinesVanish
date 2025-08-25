package com.griddb.coastal.model;

import java.time.LocalDateTime;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor 
@NoArgsConstructor
@Builder
public class WaterLevelData {
  private String stationId;
  private String stationName;
  private LocalDateTime timestamp;
  private Double waterLevel; // meters
  private String datum;
  private Double latitude;
  private Double longitude;
  private String flags; // NOAA quality flags
}