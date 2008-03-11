package org.hypergraphdb.conv.factory;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPlainLink;

public class ConverterLink extends HGPlainLink {

	private String className;

	public ConverterLink() {
	}

	public ConverterLink(HGHandle[] targets) {
		super(targets);
	}

	public ConverterLink(String name, HGHandle[] targets) {
		super(targets);
		this.className = name;
	}

	public void setClassName(String name) {
		this.className = name;
	}

	public String getClassName() {
		return className;
	}
}

