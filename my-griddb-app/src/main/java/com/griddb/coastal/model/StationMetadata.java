package com.griddb.coastal.model;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor 
@NoArgsConstructor
@Builder
public class StationMetadata {
  private String stationId;
  private String stationName;
  private String state;
  private Double latitude;
  private Double longitude;
  private String region;
  private Boolean isActive;
  private String lastUpdated;
}