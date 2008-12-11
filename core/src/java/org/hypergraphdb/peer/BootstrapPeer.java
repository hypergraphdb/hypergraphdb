package org.hypergraphdb.peer;

import java.util.Map;


/**
 * 
 * <p>
 * Implementations of this interface perform initialization operations on a 
 * <code>HyperGraphPeer</code>. The intent is to register various services
 * that the peer is able to perform, e.g. the performatives it supports, the
 * tasks it participates to etc. Multiple implementations can be provided
 * at configuration by listing them in the top-level <code>bootstrap</code>
 * JSON configuration property.  
 * </p>
 *
 * @author Borislav Iordanov
 *
 */
public interface BootstrapPeer
{
	/**
	 * <p>
	 * Perform peer initialization with the given configuration map. The
	 * configuration is specific the bootstrap instance invoked. Each implementation
	 * of this class should document the configuration that it accepts.
	 * </p>
	 * 
	 * @param peer
	 * @param config
	 */
	void bootstrap(HyperGraphPeer peer, Map<String, Object> config);
}