package org.hypergraphdb;

/**
 * 
 * <p>
 * Encapsulate the reference of an object for loading on demand. 
 * There is no standard way in Java to implement lazy evaluation
 * of method parameters. This interface serves that purpose in a simple,
 * straightforward way.
 * </p>
 * 
 * <p>
 * Concrete implementation may chose to cache the value or recompute it
 * every time. A caller should not generally rely on the value being
 * cached, even though in general that is the intent...hence the name
 * <code>LazyRef</code>.
 * </p>
 * 
 * @author Borislav Iordanov
 *
 * @param <T> The type of the object whose reference is lazyfied.
 */
public interface LazyRef<T> 
{
	/**
	 * <p>Return the actual value that this reference encapsulate.
	 * Concrete implementation may perform a length operation in 
	 * order to get to the value. 
	 * </p> 
	 */
	T deref();
}