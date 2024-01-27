package io.eteixeira.kafka.demo.processor;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@Slf4j
@Component
@RequiredArgsConstructor
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class StringProcessor {

    private final String id;

    private final BlockingQueue<String> queue = new LinkedBlockingQueue<>();

    @PostConstruct
    public void init() {
        log.info("Initializing new String Processor with id: {}", this.id);
    }

    public void processString(String value) {
        log.info("Instance {} processing new string {}.", this.id, value);
        try {
            this.queue.put(value);
        } catch (InterruptedException exception) {
            log.error("Thread has been interrupted.", exception);
            Thread.currentThread().interrupt();
        }
    }

    public Optional<String> next() {
       return Optional.ofNullable(this.queue.poll());
    }
}
