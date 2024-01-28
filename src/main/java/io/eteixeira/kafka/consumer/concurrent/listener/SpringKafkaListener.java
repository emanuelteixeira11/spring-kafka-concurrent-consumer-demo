package io.eteixeira.kafka.consumer.concurrent.listener;

import io.eteixeira.kafka.consumer.concurrent.processor.StringProcessor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.event.ConsumerStoppedEvent;
import org.springframework.stereotype.Component;

/**
 * Listener component for Kafka messages.
 * This class is responsible for processing messages received from a Kafka topic.
 * It utilizes a dedicated {@link StringProcessor} for each thread to handle incoming messages.
 */
@Slf4j
@Component
public class SpringKafkaListener {
    public static final String SPRING_KAFKA_TEST_TOPIC = "spring-kafka-test";
    private static final ThreadLocal<StringProcessor> STRING_PROCESSOR_THREAD_LOCAL = new ThreadLocal<>();

    @Autowired
    private ObjectProvider<StringProcessor> stringProcessorObjectProvider;

    /**
     * Listens for messages on the Kafka topic defined by {@code SPRING_KAFKA_TEST_TOPIC}.
     * Upon receiving a message, it delegates the processing to a dedicated {@link StringProcessor}.
     *
     * @param consumerRecord The Kafka consumer record containing the received message.
     * @param consumer The Kafka consumer instance.
     */
    @KafkaListener(topics = SPRING_KAFKA_TEST_TOPIC)
    public void onMessage(ConsumerRecord<String, String> consumerRecord, Consumer<String, String> consumer) {
        log.info("Received new message: {}", consumerRecord);
        this.executeOnDedicatedProcessor(consumerRecord, consumer);
    }

    /**
     * Executes processing of a Kafka message using a thread-specific {@link StringProcessor}.
     * It ensures that each thread uses its unique instance of StringProcessor.
     *
     * @param consumerRecord The Kafka consumer record.
     * @param consumer The Kafka consumer instance.
     */
    private void executeOnDedicatedProcessor(ConsumerRecord<String, String> consumerRecord, Consumer<String, String> consumer) {
        String processorId = consumer.groupMetadata().memberId();
        if (STRING_PROCESSOR_THREAD_LOCAL.get() == null) {
            STRING_PROCESSOR_THREAD_LOCAL.set(stringProcessorObjectProvider.getObject(processorId));
        }
        STRING_PROCESSOR_THREAD_LOCAL.get().processString(consumerRecord.value());
    }

    /**
     * Handles the {@link ConsumerStoppedEvent} to perform clean-up operations.
     * This method is called when a Kafka consumer is stopped, and it removes the
     * thread-local instance of {@link StringProcessor} associated with the consumer.
     *
     * @param consumerStoppedEvent The event triggered when the Kafka consumer is stopped.
     */
    @EventListener
    public void onEvent(ConsumerStoppedEvent consumerStoppedEvent) {
        log.info("Consumer stopped, closing String processor: {}", consumerStoppedEvent);
        STRING_PROCESSOR_THREAD_LOCAL.remove();
    }
}