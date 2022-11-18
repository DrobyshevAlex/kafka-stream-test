package ru.terraideas.results.models;

import org.apache.kafka.common.serialization.Serdes.WrapperSerde;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

public class PointsSerde extends WrapperSerde<Points> {

  public PointsSerde() {
    super(new JsonSerializer<>(), new JsonDeserializer<>(Points.class));
  }
}
