package org.hypergraphdb.conv.factory;

public class ConstructorDescriptor {
	 private String[] params;

	public ConstructorDescriptor() {
	}

	public ConstructorDescriptor(String[] params) {
		this.params = params;
	}

	public String[] getParams() {
		return params;
	}

	public void setParams(String[] params) {
		this.params = params;
	}
}
