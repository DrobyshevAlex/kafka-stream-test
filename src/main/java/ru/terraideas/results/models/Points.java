package ru.terraideas.results.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.With;

@Data
@Builder
@With
@AllArgsConstructor
@NoArgsConstructor
public class Points {
  private long eventId;
  private long divisionId;
  private long slotId;
  private long points;
  private long maxPoints;
  private double time;
  private double hf;
  private double maxHf;
  private double stagePoints;
}
