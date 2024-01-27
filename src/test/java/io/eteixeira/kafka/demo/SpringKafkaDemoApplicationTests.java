package io.eteixeira.kafka.demo;

import org.apache.kafka.clients.admin.NewTopic;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.IntStream;

@SpringBootTest(classes = {SpringKafkaDemoApplicationTests.TestKafkaDemoApplicationConfiguration.class, SpringKafkaDemoApplication.class})
class SpringKafkaDemoApplicationTests {

	@Autowired
	private KafkaTemplate<Object, Object> kafkaTemplate;

	@Test
	void all_messages_processed_by_multiple_string_processor_instances() {
		Assertions.assertThat(kafkaTemplate).isNotNull();
		var futures = IntStream.range(0, 1000)
				.mapToObj(value -> "key_" + value % 10)
				.map(key -> this.kafkaTemplate.send("spring-kafka-test", key, "data"))
						.toArray(CompletableFuture[]::new);
		Assertions.assertThat(CompletableFuture.allOf(futures)).succeedsWithin(Duration.ofSeconds(10));
	}


	@TestConfiguration
	public static class TestKafkaDemoApplicationConfiguration {

		@Bean
		@ServiceConnection
		KafkaContainer kafkaContainer() {
			return new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:latest"));
		}

		@Bean
		Function<MessageListenerContainer, String> threadNameSupplier() {
			return messageListenerContainer -> messageListenerContainer.getGroupId() + "-" +
							Arrays.asList(messageListenerContainer.getListenerId().split("-")).getLast();
		}

		@Bean
		NewTopic springKafkaTestTopic() {
			return TopicBuilder.name("spring-kafka-test")
					.partitions(5)
					.build();
		}
	}
}
