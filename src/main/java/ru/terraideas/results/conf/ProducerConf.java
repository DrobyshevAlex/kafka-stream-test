package ru.terraideas.results.conf;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

@Configuration
public class ProducerConf {
  public static final String IN_TOPIC_NAME = "event-points";
  public static final String OUT_TOPIC_NAME = "event-results";

  @Bean
  public ProducerFactory<Long, Object> producerFactory() {
    Map<String, Object> config = new HashMap<>();
    config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
    return new DefaultKafkaProducerFactory<>(config);
  }

  @Bean
  public KafkaTemplate<Long, Object> kafkaTemplate() {
    return new KafkaTemplate<>(producerFactory());
  }

  @Bean
  public NewTopic inTopic() {
    return new NewTopic(IN_TOPIC_NAME, 4, (short) 1);
  }

  @Bean
  public NewTopic outTopic() {
    return new NewTopic(OUT_TOPIC_NAME, 4, (short) 1);
  }
}
