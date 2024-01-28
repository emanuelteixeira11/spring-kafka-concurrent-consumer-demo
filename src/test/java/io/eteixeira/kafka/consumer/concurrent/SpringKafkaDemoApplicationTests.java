package io.eteixeira.kafka.consumer.concurrent;

import io.eteixeira.kafka.consumer.concurrent.processor.StringProcessor;
import lombok.NonNull;
import org.apache.kafka.clients.admin.NewTopic;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.config.BeanPostProcessor;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.IntStream;

import static io.eteixeira.kafka.consumer.concurrent.listener.SpringKafkaListener.SPRING_KAFKA_TEST_TOPIC;

@SpringBootTest(classes = {SpringKafkaDemoApplicationTests.TestKafkaDemoApplicationConfiguration.class, SpringKafkaDemoApplication.class})
class SpringKafkaDemoApplicationTests {

	@Autowired
	private KafkaTemplate<Object, Object> kafkaTemplate;

	@Autowired
	@Qualifier("stringProcessors")
	private List<StringProcessor> stringProcessors;

	@Test
	void all_messages_processed_by_multiple_string_processor_instances() {
		Assertions.assertThat(kafkaTemplate).isNotNull();
		var futures = IntStream.range(0, 1000)
				.mapToObj(value -> "key_" + value % 10)
				.map(key -> this.kafkaTemplate.send(SPRING_KAFKA_TEST_TOPIC, key, "data for key: " + key))
						.toArray(CompletableFuture[]::new);
		Assertions.assertThat(CompletableFuture.allOf(futures)).succeedsWithin(Duration.ofSeconds(10));
		Awaitility.await().untilAsserted(() -> Assertions.assertThat(this.stringProcessors).hasSize(2));
		Assertions.assertThat(stringProcessors.getFirst().queueSize()).isPositive();
		Assertions.assertThat(stringProcessors.getLast().queueSize()).isPositive();
		Assertions.assertThat(stringProcessors.getLast().queuedData())
				.doesNotContainAnyElementsOf(stringProcessors.getFirst().queuedData());
	}


	@TestConfiguration
	public static class TestKafkaDemoApplicationConfiguration {

		@Bean("stringProcessors")
		List<StringProcessor> stringProcessors() {
			return new ArrayList<>();
		}

		@Bean
		BeanPostProcessor beanPostProcessor(@Qualifier("stringProcessors") List<StringProcessor> stringProcessors) {
			return new BeanPostProcessor() {
				@Override
				public Object postProcessAfterInitialization(@NonNull Object bean, @NonNull String beanName) throws BeansException {
					if (bean instanceof StringProcessor stringProcessor) {
						stringProcessors.add(stringProcessor);
					}
					return bean;
				}
			};
		}

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
			return TopicBuilder.name(SPRING_KAFKA_TEST_TOPIC)
					.partitions(5)
					.build();
		}
	}
}
