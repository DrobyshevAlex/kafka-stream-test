package ru.terraideas.results;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.core.KafkaTemplate;

import ru.terraideas.results.conf.ProducerConf;
import ru.terraideas.results.models.Points;
import ru.terraideas.results.models.PointsSupplier;

import lombok.AllArgsConstructor;
import lombok.SneakyThrows;

@SpringBootApplication
@AllArgsConstructor
public class ProducerApplication implements CommandLineRunner {
	private final KafkaTemplate<Long, Object> template;
	private final PointsSupplier pointsSupplier;

	public static void main(String[] args) {
		SpringApplication.run(ProducerApplication.class, args);
	}

	@Override
	public void run(String... args) {
		send();
	}

	@SneakyThrows
	private void send() {
		for (int k = 0; k < 100000; ++k) {
			// long eventId = System.currentTimeMillis();
			long eventId = k * 5 + 1;
			for (int i = 0; i < 5; ++i) {
				Points model = pointsSupplier.get()
						.withEventId(eventId)
						.withSlotId(eventId + i);
				template.send(
						ProducerConf.IN_TOPIC_NAME, model.getEventId(),
						model.withHf(model.getPoints() / model.getTime()));
			}

			Thread.sleep(3000);
		}
	}
}
