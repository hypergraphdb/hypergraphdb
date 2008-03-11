package org.hypergraphdb.conv.factory;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPlainLink;

public class MethodDescriptor {

	private String name;
    private String[] params;
    
	public MethodDescriptor() {
	}

	public MethodDescriptor(String name, String[] params) {
		this.name = name;
		this.params = params;
	}

	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
	}
	public String[] getParams() {
		return params;
	}
	public void setParams(String[] params) {
		this.params = params;
	}
	
}
