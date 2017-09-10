package org.apache.kafka.streams;

import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;

/**
 * User: Bill Bejeck
 * Date: 9/9/17
 * Time: 3:06 PM
 */
public class InternalTopologyAccess {

    private final Topology topology;

    public InternalTopologyAccess(Topology topology) {
        this.topology = topology;
    }

    public InternalTopologyBuilder getInternalBuilder() {
          return topology.internalTopologyBuilder;
    }
}
