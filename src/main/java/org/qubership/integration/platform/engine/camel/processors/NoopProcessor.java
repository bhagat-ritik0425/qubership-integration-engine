package org.qubership.integration.platform.engine.camel.processors;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.springframework.stereotype.Component;

@Component
public class NoopProcessor implements Processor {
    @Override
    public void process(Exchange exchange) throws Exception {

    }
}
