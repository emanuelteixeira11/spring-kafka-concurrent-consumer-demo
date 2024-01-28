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

/**
 * Tests for the Spring Kafka Demo Application.
 * This class includes tests to verify the proper functioning of KafkaTemplate and StringProcessor beans.
 */
@SpringBootTest(classes = {SpringKafkaDemoApplicationTests.TestKafkaDemoApplicationConfiguration.class, SpringKafkaDemoApplication.class})
class SpringKafkaDemoApplicationTests {

    @Autowired
    private KafkaTemplate<Object, Object> kafkaTemplate;

    @Autowired
    @Qualifier("stringProcessors")
    private List<StringProcessor> stringProcessors;
    private static final int NUM_MESSAGES = 1000;
    private static final int PARTITION_KEY_DIVISOR = 10;

    /**
     * Tests if all messages are processed by multiple StringProcessor instances.
     * Ensures that the KafkaTemplate is not null, sends a number of messages, and
     * then verifies that all messages are processed within a specified duration and
     * that each StringProcessor instance has a non-empty queue with distinct data.
     */
    @Test
    void testAllMessagesProcessedByMultipleStringProcessors() {
        Assertions.assertThat(kafkaTemplate).isNotNull();
        CompletableFuture<?>[] futures = sendMessages();
        assertAllMessagesProcessed(futures);
        assertStringProcessorsBehaviors();
    }

    /**
     * Sends a predefined number of messages to a Kafka topic.
     * Each message has a key derived from a modulus operation and a specific data payload.
     *
     * @return an array of CompletableFuture objects representing the result of each send operation.
     */
    private CompletableFuture<?>[] sendMessages() {
        return IntStream.range(0, NUM_MESSAGES)
                .mapToObj(value -> "key_" + value % PARTITION_KEY_DIVISOR)
                .map(key -> kafkaTemplate.send(SPRING_KAFKA_TEST_TOPIC, key, "data for key: " + key))
                .toArray(CompletableFuture[]::new);
    }

    /**
     * Asserts that all sent messages are processed within a specific time frame.
     *
     * @param futures an array of CompletableFuture objects representing the result of each send operation.
     */
    private void assertAllMessagesProcessed(CompletableFuture<?>[] futures) {
        Assertions.assertThat(CompletableFuture.allOf(futures)).succeedsWithin(Duration.ofSeconds(10));
    }

    /**
     * Asserts specific behaviors of the StringProcessor instances.
     * Verifies that there are exactly two StringProcessor instances, each with a non-empty queue,
     * and that the data in the queues of these two processors are distinct.
     */
    private void assertStringProcessorsBehaviors() {
        Awaitility.await().untilAsserted(() -> Assertions.assertThat(stringProcessors).hasSize(2));
        Assertions.assertThat(stringProcessors.get(0).queueSize()).isPositive();
        Assertions.assertThat(stringProcessors.get(1).queueSize()).isPositive();
        Assertions.assertThat(stringProcessors.get(1).distinctQueuedData())
                .doesNotContainAnyElementsOf(stringProcessors.get(0).distinctQueuedData());
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
