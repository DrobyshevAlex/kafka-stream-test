package ru.terraideas.results.conf;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

import ru.terraideas.results.models.Points;
import ru.terraideas.results.models.PointsSerde;

@Configuration
public class TopologyConfiguration {
  public static final String IN_TOPIC_NAME = "event-points";
  public static final String OUT_TOPIC_NAME = "event-results";
  public static final String STORE_NAME = "event-results-store";

  @Bean
  public Topology createTopology(StreamsBuilder streamsBuilder) {

    KStream<Long, Points> inPoints = streamsBuilder.stream(IN_TOPIC_NAME,
        Consumed.with(Serdes.Long(),
            new PointsSerde()));

    inPoints = inPoints.mapValues((p) -> p.withHf(p.getPoints() / p.getTime()));

    var maxHFTable = inPoints.groupByKey()
        .aggregate(() -> 0.0, (k, v, r) -> Math.max(r, v.getHf()), Materialized.with(Serdes.Long(), Serdes.Double()));

    var resStream = inPoints
        .join(maxHFTable, (k, p, hf) -> p.withMaxHf(hf))
        .mapValues((p) -> p.withStagePoints(p.getMaxPoints() * p.getHf() / p.getMaxHf()));

    // var resTable = resStream.map((k, v) -> KeyValue.pair(v.getSlotId(),
    // v)).toTable();

    resStream.peek((k1, v1) -> System.out.printf("peekout stream flat %d\n", v1.getSlotId()));

    resStream.to(OUT_TOPIC_NAME, Produced.with(Serdes.Long(), new JsonSerde<>(Points.class)));

    Topology topology = streamsBuilder.build();
    System.out.println("*** TOPOLOGY *** ");
    System.out.println(topology.describe());
    System.out.println("******");
    return topology;
  }
}