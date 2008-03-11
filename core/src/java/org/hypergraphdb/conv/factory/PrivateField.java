package org.hypergraphdb.conv.factory;

public class PrivateField {

	private String name;

	public PrivateField() {
	}
	
	public PrivateField(String name) {
		this.name = name;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
}
