package io.eteixeira.kafka.consumer.concurrent.processor;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Processor class for handling string data.
 * This class is responsible for processing and storing string values.
 * It is scoped as a prototype bean in the Spring context.
 */
@Slf4j
@Component
@RequiredArgsConstructor
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class StringProcessor {

    private final String id;
    private final BlockingQueue<String> queue = new LinkedBlockingQueue<>();

    /**
     * Initializes the StringProcessor instance.
     * This method logs the creation of a new StringProcessor instance with its unique ID.
     */
    @PostConstruct
    public void init() {
        log.info("Initializing new String Processor with id: {}", this.id);
    }

    /**
     * Processes and stores a string value in a queue.
     * Logs the processing action and handles any potential interruption during the process.
     *
     * @param value The string value to be processed.
     */
    public void processString(String value) {
        log.info("Instance {} processing new string {}.", this.id, value);
        try {
            this.queue.put(value);
        } catch (InterruptedException exception) {
            log.error("Thread has been interrupted.", exception);
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Retrieves the current size of the queue.
     *
     * @return The number of elements in the queue.
     */
    public int queueSize() {
        return this.queue.size();
    }

    /**
     * Returns a set of distinct strings from the queue.
     * This method is useful for retrieving all unique values that have been processed.
     *
     * @return A Set of distinct strings.
     */
    public Set<String> distinctQueuedData() {
        return new HashSet<>(this.queue);
    }
}
