package com.arkmq;

import org.apache.camel.builder.RouteBuilder;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class ProducerRoute extends RouteBuilder {

    @Override
    public void configure() throws Exception {
        // Send 10,000 messages to configured queue (via producer.queue property)
        from("timer:producer?repeatCount=10000&period=10")
            .routeId("jms-producer")
            .setBody(simple("Message ${exchangeProperty.CamelTimerCounter} at ${date:now:yyyy-MM-dd HH:mm:ss}"))
            .log("Sending message ${exchangeProperty.CamelTimerCounter}")
            .to("jms:queue:{{producer.queue}}")
            .log("Sent message ${exchangeProperty.CamelTimerCounter}");
    }
}

