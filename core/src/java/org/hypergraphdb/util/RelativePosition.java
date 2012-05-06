package org.hypergraphdb.util;

public enum RelativePosition {
	FIRST(1),
	LAST(1),
	NOT_FIRST(-1),
	NOT_LAST(-1);
	
	private final long expectedSize;
	
	RelativePosition(final long expectedSize) {
		this.expectedSize = expectedSize;
	}

	public long getExpectedSize(long maxSize) {
		if (expectedSize >= 0) {
			return expectedSize;
		}
		return maxSize + expectedSize;  //actually negative so it's like a minus
	}
}
