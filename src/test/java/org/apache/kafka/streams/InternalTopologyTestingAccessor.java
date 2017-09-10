package org.apache.kafka.streams;

import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;

/**
 * User: Bill Bejeck
 * Date: 9/9/17
 * Time: 3:06 PM
 */
public final class InternalTopologyTestingAccessor {

    private InternalTopologyTestingAccessor() {}

    public static InternalTopologyBuilder getInternalBuilderForTesting(Topology topology) {
          return topology.internalTopologyBuilder;
    }
}
