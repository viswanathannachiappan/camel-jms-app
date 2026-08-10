package com.arkmq;

import org.apache.camel.builder.RouteBuilder;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.concurrent.atomic.AtomicInteger;

@ApplicationScoped
public class PipelineRoute extends RouteBuilder {


    private final AtomicInteger messageCount = new AtomicInteger(0);

    @Override
    public void configure() throws Exception {

        // Automatically creates 15,000 test messages (10 per second) and stops
        from("timer:generator?period=100&repeatCount=15000")
                .routeId("test-data-generator")
                .setBody(constant("Auto-generated Test Order"))
                // Drop it into the start of the pipeline (ORDERS.NEW)
                // Drop it into the start of the pipeline (ORDERS.SHIPPED)
                // Drop it into the start of the pipeline (ORDERS.CUSTOMERS)
                .to("jms:queue:{{consumer.queue}}");

        from("jms:queue:{{consumer.queue}}?concurrentConsumers=5")
                .routeId("updated-app")

                .process(exchange -> {
                    int currentCount = messageCount.incrementAndGet();
                    String originalBody = exchange.getIn().getBody(String.class);

                    String processedBody = originalBody + " -> [PROCESSED BY APP]";
                    exchange.getIn().setBody(processedBody);

                    // Log progress
                    if (currentCount % 100 == 0) {
                        log.info("Successfully bridged {} orders...", currentCount);
                    }
                })
                .log("About to send to {{producer.queue}}: ${body}")
                // Drop the finished order into the end of the pipeline (ORDERS.PROCESSED)
                // Drop the finished order into the end of the pipeline (ORDERS.DELIVERED)
                // Drop the finished order into the end of the pipeline (ORDERS.RETURNS)
                .to("jms:queue:{{producer.queue}}")
                .log("Successfully sent to {{producer.queue}}");
    }
}