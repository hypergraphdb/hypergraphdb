package org.hypergraphdb.storage.lmdb;

public class LMDBUtils {
	public static void checkArgNotNull(Object value, String name) {
	    if (value == null) {
	        throw new IllegalArgumentException("The " + name + " argument cannot be null");
	    }
	}
}
