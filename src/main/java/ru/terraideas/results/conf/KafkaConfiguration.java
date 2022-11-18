package ru.terraideas.results.conf;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

import ru.terraideas.results.models.PointsSerde;

@Configuration
public class KafkaConfiguration {
  @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
  public KafkaStreamsConfiguration getStreamsConfig() {
    Map<String, Object> props = new HashMap<>();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "calc-results-complete");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, PointsSerde.class.getName());
    props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
        LogAndContinueExceptionHandler.class);
    props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
    KafkaStreamsConfiguration streamsConfig = new KafkaStreamsConfiguration(props);
    return streamsConfig;
  }

}
