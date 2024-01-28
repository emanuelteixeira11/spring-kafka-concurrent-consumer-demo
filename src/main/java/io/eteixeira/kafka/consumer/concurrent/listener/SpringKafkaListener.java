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

@Slf4j
@Component
public class SpringKafkaListener {
    public static final String SPRING_KAFKA_TEST_TOPIC = "spring-kafka-test";
    private static final ThreadLocal<StringProcessor> STRING_PROCESSOR_THREAD_LOCAL = new ThreadLocal<>();
    @Autowired
    private ObjectProvider<StringProcessor> stringProcessorObjectProvider;

    @KafkaListener(topics = SPRING_KAFKA_TEST_TOPIC)
    public void onMessage(ConsumerRecord<String, String> consumerRecord, Consumer<String, String> consumer) {
        log.info("Received new message: {}", consumerRecord);
        this.executeOnDedicatedProcessor(consumerRecord, consumer);
    }

    private void executeOnDedicatedProcessor(ConsumerRecord<String, String> consumerRecord, Consumer<String, String> consumer) {
        String processorId = consumer.groupMetadata().memberId();
        if (STRING_PROCESSOR_THREAD_LOCAL.get() == null) {
            STRING_PROCESSOR_THREAD_LOCAL.set(stringProcessorObjectProvider.getObject(processorId));
        }
        STRING_PROCESSOR_THREAD_LOCAL.get().processString(consumerRecord.value());
    }

    @EventListener
    public void onEvent(ConsumerStoppedEvent consumerStoppedEvent) {
        log.info("Consumer stopped, closing String processor: {}", consumerStoppedEvent);
        STRING_PROCESSOR_THREAD_LOCAL.remove();
    }
}