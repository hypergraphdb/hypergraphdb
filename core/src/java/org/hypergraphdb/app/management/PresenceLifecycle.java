package org.hypergraphdb.app.management;

import org.hypergraphdb.HyperGraph;

/**
 * 
 * <p>
 * This interface defines the operations governing the lifecycle of a component
 * in a HyperGraph instance. Typically, the component is an application that comes
 * packaged as a JAR archive and contains a set of types, events, instances etc. 
 * However, it may be some component at another level of granularity (finer or coarser). 
 * </p>
 *
 * <p>
 * The <code>install</code> and <code>uninstall</code> operation are mandatory for
 * all classes implementing this interface. The other operations are optional and
 * may throw an <code>UnsupportedOperationException</code>.
 * </p>
 * 
 * @author Borislav Iordanov
 *
 */
public interface PresenceLifecycle
{
	/**
	 * <p>
	 * Install assumes that the entity being installed is not present in the 
	 * HyperGraph instance. 
	 * </p>
	 * 
	 * @param graph
	 */
	void install(HyperGraph graph);
	
	/**
	 * <p>
	 * Uninstall removes all atoms deemed part of the entity being uninstalled
	 * from the HyperGraph instance.
	 * </p>
	 * 
	 * @param graph
	 */
	void uninstall(HyperGraph graph);
	
	/**
	 * <p>
	 * Updates an existing installation with a newer version. What the update
	 * actually does, and whether it is even implemented depends on the
	 * concrete entity being updated.  For example, an application that has
	 * a newer implementation of a given type might replace all instances of the
	 * old version of the type with equivalent instances of the new version.
	 * </p>
	 * 
	 * <p>
	 * The intent of <code>update</code> is to preserve any existing data when
	 * installing a newer version of a component. It is an <em>optional</em>
	 * operation.
	 * </p>
	 * 
	 * @param graph
	 */
	void update(HyperGraph graph);
	
	/**
	 * <p>
	 * Reset should have the same effect as an uninstall followed by an install.
	 * The intent of reset is the start from a clean slate without having to fully
	 * reinstall an application. It is <em>optional</code> operation. 
	 * </p>
	 * @param graph
	 */
	void reset(HyperGraph graph);	
}