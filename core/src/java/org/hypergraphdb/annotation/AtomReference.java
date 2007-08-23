package org.hypergraphdb.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface AtomReference
{
	/**
	 * The single attribute of this annotation is the symbolic constant
	 * of a <code>HGAtomRef.Mode</code>, specified as a string. This is, 
	 * one of "hard", "symbolic" or "floating".
	 */
	String value();
}