package com.arkmq;

import org.apache.camel.builder.RouteBuilder;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.concurrent.atomic.AtomicInteger;

@ApplicationScoped
public class ConsumerRoute extends RouteBuilder {

    private final AtomicInteger messageCount = new AtomicInteger(0);

    @Override
    public void configure() throws Exception {
        // Consume messages from configured queue (via consumer.queue property)
        from("jms:queue:{{consumer.queue}}?concurrentConsumers=5&maxConcurrentConsumers=10")
                .routeId("jms-consumer")
                .process(exchange -> {
                    int currentCount = messageCount.incrementAndGet();

                    exchange.setProperty("currentCount", currentCount);

                    if (currentCount % 100 == 0) {
                        log.info("Processed {} messages so far...", currentCount);
                    }
                })
                .log("Received: ${body}")
                .log("Total messages processed: ${exchangeProperty.currentCount}");
    }
}