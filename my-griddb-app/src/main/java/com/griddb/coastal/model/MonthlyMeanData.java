package com.griddb.coastal.model;

import java.time.LocalDate;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor 
@NoArgsConstructor
@Builder
public class MonthlyMeanData {
  private String stationId;
  private String stationName;
  private LocalDate month;
  private Double meanSeaLevel; // meters above MSL
  private Integer year;
  private Integer monthNumber;
  private Double latitude;
  private Double longitude;
}