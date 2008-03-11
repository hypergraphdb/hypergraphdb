package org.hypergraphdb.conv.types;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPlainLink;

public class AddOnLink extends HGPlainLink {

	public AddOnLink() {
	}

	public AddOnLink(HGHandle[] outgoingSet) {
		super(outgoingSet);
	}
}
