/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.peer;

import mjson.Json;


/**
 * 
 * <p>
 * Implementations of this interface perform initialization operations on a 
 * <code>HyperGraphPeer</code>. The intent is to register various services
 * that the peer is able to perform, e.g. the
 * tasks it participates to etc. Multiple implementations can be provided
 * at configuration time by listing them in the top-level <code>bootstrap</code>
 * JSON configuration property.  
 * </p>
 *
 * <p>
 * Note that bootstrap operations are performed <strong>before</strong> the peer
 * joins the peer network, so they cannot rely messaging with other peers in
 * order to complete. 
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
	 * configuration is specific to the bootstrap instance invoked. Each implementation
	 * of this class should document the configuration that it accepts.
	 * </p>
	 * 
	 * @param peer
	 * @param config
	 */
	void bootstrap(HyperGraphPeer peer, Json config);
}