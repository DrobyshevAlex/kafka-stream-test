package ru.terraideas.results.models;

import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;

import org.springframework.stereotype.Service;

@Service
public class PointsSupplier implements Supplier<Points> {
  @Override
  public Points get() {
    return Points.builder()
        .divisionId(ThreadLocalRandom.current().nextInt(2))
        .maxPoints(100)
        .points(ThreadLocalRandom.current().nextInt(100))
        .time(ThreadLocalRandom.current().nextDouble() * 100)
        .build();
  }
}
