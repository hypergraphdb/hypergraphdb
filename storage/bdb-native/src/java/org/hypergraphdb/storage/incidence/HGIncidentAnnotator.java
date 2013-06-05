package org.hypergraphdb.storage.incidence;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.HyperGraph;

public interface HGIncidentAnnotator
{
    int spaceNeeded(HyperGraph graph);
    void annotate(HyperGraph graph, HGHandle link, HGHandle target, byte [] data, int offset);
    HGRandomAccessResult<HGHandle> lookup(HyperGraph graph, HGRandomAccessResult<byte[]> rs, Object...annotations);
}
