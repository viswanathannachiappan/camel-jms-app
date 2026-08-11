package com.arkmq;

import org.apache.camel.builder.RouteBuilder;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
public class MasterSinkRoute extends RouteBuilder {

    @ConfigProperty(name = "consumer.queue")
    String consumerQueues;

    @Override
    public void configure() throws Exception {
        // Split comma-separated queues passed from CONSUMER_QUEUE
        String[] queues = consumerQueues.split(",");

        for (String queue : queues) {
            String trimmedQueue = queue.trim();
            if (trimmedQueue.isEmpty()) continue;

            // Dynamically register 5 concurrent consumers for each queue in the list
            fromF("jms:queue:%s?concurrentConsumers=5", trimmedQueue)
                    .routeId("drain-sink-" + trimmedQueue.toLowerCase().replace('.', '-'))
                    .log("Successfully consumed and deleted message from " + trimmedQueue + ": ${body}");
        }
    }
}