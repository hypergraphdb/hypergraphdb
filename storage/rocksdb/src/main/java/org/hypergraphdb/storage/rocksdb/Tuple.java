/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see
 * the LicensingInformation file at the root level of the distribution.
 *
 * Copyright (c) 2005-2024 Kobrix Software, Inc.  All rights reserved.
 *
 */

package org.hypergraphdb.storage.rocksdb;

public class Tuple
{
	public static class Pair<A,B>
	{
		public final A a;
		public final B b;
		public Pair(A a, B b)
		{
			this.a = a;
			this.b = b;
		}

	}

	public static class Triplet<A,B,C>
	{
		public final A a;
		public final B b;
		public final C c;
		private Triplet(A a, B b, C c)
		{
			this.a = a;
			this.b = b;
			this.c = c;
		}

	}
	public static <A, B> Pair<A,B> pair(A a, B b)
	{
		return new Pair<>(a,b);
	}

	public static <A, B, C> Triplet<A,B,C> triplet(A a, B b, C c)
	{
		return new Triplet(a,b,c);
	}

}
