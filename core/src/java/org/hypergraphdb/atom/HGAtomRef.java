/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see 
 * the LicensingInformation file at the root level of the distribution. 
 *
 * Copyright (c) 2005-2006
 *  Kobrix Software, Inc.  All rights reserved.
 */
package org.hypergraphdb.atom;

import org.hypergraphdb.HGHandle;

/**
 * <p>
 * An instance <code>HGAtomRef</code> represents a reference to a HyperGraph atom. Atom
 * references can be used as atom values or as values of projections of composite types.
 * </p>
 * 
 * <p>
 * An <code>HGAtomRef</code> is more than a <code>HGHandle</code> because it has a special
 * relationship with its referent and <strong>may</strong> play a role in the latter's lifetime. 
 * While a <code>HGHandle</code> can be thought of as a plain pointer, a <code>HGAtomRef</code>
 * is more like a <em>smart pointer</em>. When a <code>HGAtomRef</code> is removed from HyperGraph,
 * the underlying referent might be affected depending on the <em>mode</em> of the reference. 
 * </p>
 * 
 * <p>
 * The mode of an atom reference defines how it affects the lifetime of the atom it refers to.
 * There are several possibilities and an application must choose the most suitable one:
 * 
 * <ol>
 * <li>The referent is an atom that is mainly accessed through its <code>HGAtomRef</code>s and
 * generally has no purpose of being outside of that context.</li>
 * <li>The referent has no particular relationship to the referee, but one needs to explicitly
 * represent a reference to an atom for the purposes automatic dereferencing, as opposed to just
 * storing a handle which is a pure reference value.</li>
 * <li>The reference is an atom that may carry valuable information that is accessible through
 * queries and other means, and it is possible to re-establish atom references to it even after
 * their removal.</li>
 * </ol>
 * 
 * The above three cases are represented by the three possible modes of an atom reference:
 * <code>HARD</code>, <code>SYMBOLIC</code> and <code>FLOATING</code> respectively. The 
 * terms <code>HARD</code> and <code>SYMBOLIC</code> were chosen because of their familiarity
 * from Unix and derivative filesystems since atom references with those modes behave like
 * the corresponding file links in those systems.  The term <code>FLOATING</code> is specific to 
 * HyperGraph and it has the effect of transforming a refered to atom to a temporary, managed 
 * atom that gets removed if not used.
 * </p>
 * 
 * @author Borislav Iordanov
 */
public class HGAtomRef 
{
	public enum Mode
	{
		/**
		 * This constant define a <code>HARD</code> reference mode. This behavior of  
		 * hard references is like the Unix filesystem hard links: when all hard references
		 * are removed from HyperGraph, the referent is removed as well.
		 */		
		hard((byte)0),
		
		/**
		 * This constant define a <code>SYMBOLIC</code> reference mode. This behavior of  
		 * symbolic references is like the Unix filesystem symbolic links: they are just pointers
		 * to the referent without an effect on its lifetime.
		 */
		symbolic((byte)1),
		
		/**
		 * This constant define an <code>FLOATING</code> reference mode. This behavior of  
		 * floating references is particular to HyperGraph: when removed they will leave the
		 * referent as a temporary atom that will eventually be automatically removed
		 * from HyperGraph if not used. This behavior relies on the <code>MANAGED</code>
		 * system-level atom attribute. If the referent is not managed when the last floating reference
		 * is removed, it will be tagged as managed from then on. Thus it is still possible to
		 * access the atom by some other means (e.g. a handle to it is kept somewhere or through
		 * a query) and re-establish a <code>HGAtomRef</code> to it. 
		 */
		floating((byte)2);
			
		private byte code;
		private Mode(byte code) { this.code = code; }
		
		public byte getCode() { return code; }
		public static Mode get(byte code) 
		{ if (code == 0) return hard; else if (code == 1) return symbolic; else return floating; }
	}
	
	private HGHandle referent;
	private Mode mode;
	
	/**
	 * <p>Construct a new <code>HGAtomRef</code> to the atom pointed by <code>reference<code>
	 * and with the specified <code>mode</code>.
	 * 
	 * @param referent The <code>HGHandle</code> of the refered to atom.
	 * @param mode The atom reference mode.
	 * this class.
	 */
	public HGAtomRef(HGHandle referent, Mode mode)
	{
		this.referent = referent;
		this.mode = mode;
	}
	
	/**
	 * <p>Return the atom reference mode.</p>
	 */
	public Mode getMode()
	{
		return mode;
	}
	
	/**
	 * <p>Return <code>true</code> if this is a hard reference and <code>false</code> otherwise.</p>
	 */
	public boolean isHard()
	{
		return mode == Mode.hard;
	}
	
	/**
	 * <p>Return <code>true</code> if this is a symbolic reference and <code>false</code> otherwise.</p>
	 */
	public boolean isSymbolic()
	{
		return mode == Mode.symbolic;
	}
	
	/**
	 * <p>Return <code>true</code> if this is a floating reference and <code>false</code> otherwise.</p>
	 */
	public boolean isFloating()
	{
		return mode == Mode.floating;
	}
	
	/**
	 * <p>Return the referent atom.</p>
	 */
	public HGHandle getReferent()
	{
		return referent;
	}
}