package ru.terraideas.results;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.support.serializer.JsonSerde;

import ru.terraideas.results.models.Points;
import ru.terraideas.results.models.PointsSerde;
import ru.terraideas.results.conf.KafkaConfiguration;
import ru.terraideas.results.conf.TopologyConfiguration;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestTopology {

  private TopologyTestDriver topologyTestDriver;
  private TestInputTopic<Long, Points> inTopic;
  private TestOutputTopic<Long, Points> outTopic;
  private JsonSerde<Points> pointsDes;

  @BeforeEach
  public void setUp() throws IOException {
    pointsDes = new JsonSerde<>(Points.class);

    KafkaStreamsConfiguration config = new KafkaConfiguration().getStreamsConfig();
    config.asProperties().put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());
    config.asProperties().put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, PointsSerde.class.getName());

    StreamsBuilder sb = new StreamsBuilder();
    Topology topology = new TopologyConfiguration().createTopology(sb);

    topology.addStateStore(
        Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(TopologyConfiguration.STORE_NAME),
            Serdes.Long(),
            pointsDes));

    topologyTestDriver = new TopologyTestDriver(topology, config.asProperties());

    inTopic = topologyTestDriver.createInputTopic(TopologyConfiguration.IN_TOPIC_NAME, Serdes.Long().serializer(),
        pointsDes.serializer());

    outTopic = topologyTestDriver.createOutputTopic(TopologyConfiguration.OUT_TOPIC_NAME, Serdes.Long().deserializer(),
        pointsDes.deserializer());
  }

  @AfterEach
  public void tearDown() {
    // pointsDes.close();
    topologyTestDriver.close();
  }

  void send(Points value) {
    inTopic.pipeInput(value.getEventId(), value);
  }

  @Test
  void testTopology() {
    send(Points.builder()
        .eventId(1)
        .slotId(1)
        .maxPoints(5)
        .points(5)
        .time(10)
        .build());

    send(Points.builder()
        .eventId(1)
        .slotId(2)
        .maxPoints(5)
        .points(5)
        .time(5)
        .build());

    var p = outTopic.readValue();
    assertEquals(1L, p.getSlotId());
    assertEquals(5L, p.getPoints());
    assertEquals(0.5, p.getHf());
    assertEquals(0.5, p.getMaxHf());
    assertEquals(5.0, p.getStagePoints());

    // p = outTopic.readValue();
    // assertEquals(1L, p.getSlotId());
    // assertEquals(5L, p.getPoints());
    // assertEquals(0.5, p.getHf());
    // assertEquals(1.0, p.getMaxHf());
    // assertEquals(2.5, p.getStagePoints());

    p = outTopic.readValue();
    assertEquals(2L, p.getSlotId());
    assertEquals(5L, p.getPoints());
    assertEquals(1.0, p.getHf());
    assertEquals(1.0, p.getMaxHf());
    assertEquals(5.0, p.getStagePoints());

    assertTrue(outTopic.isEmpty());
  }
}
