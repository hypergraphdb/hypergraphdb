/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.peer.jxta;

public class JXTAConfig
{
	public static final String CONFIG_NAME = "jxta";
	public static final String MODE = "mode";
	public static final String GROUP_NAME = "peerGroup";
	public static final String NEEDS_RENDEZ_VOUS = "needsRendezVous";
	public static final String NEEDS_RELAY = "needsRelay";
	public static final String ADVERTISEMENT_TTL = "advertisementTTL";
	public static final String JXTA_DIR = "jxtaDir";

	public static final String TCP = "tcp";
	public static final Object HTTP = "http";
	
	public static final Object ENABLED = "enabled";
	public static final Object INCOMING = "incoming";
	public static final Object OUTGOING = "outgoing";
	public static final Object PORT = "port";
	public static final Object START_PORT = "startPort";
	public static final Object END_PORT = "endPort";
	
	public static final Object RELAYS = "relays";
	public static final Object RENDEZVOUS = "rdvs";
	

	public static final int DEFAULT_TCP_PORT = 9801;
	public static final int DEFAULT_HTTP_PORT = 9802;
}
