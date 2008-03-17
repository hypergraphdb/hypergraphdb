package org.hypergraphdb;

/**
 * 
 * <p>
 * A bean that holds configuration parameters for a HyperGraphDB initialization.
 * An instance can be passed to the <code>HGEnvironment.configure</code> or 
 * <code>HGEnvironment.get</code> methods.
 * </p>
 *
 * @author Borislav Iordanov
 *
 */
public class HGConfiguration
{
	private boolean transactional;

	public HGConfiguration()
	{
		resetDefaults();
	}
	
	/**
	 * <p>Set all parameters of this configuration to their default values.</p>
	 */
	public void resetDefaults()
	{
		this.transactional = true;
	}
	
	public boolean isTransactional()
	{
		return transactional;
	}

	public void setTransactional(boolean transactional)
	{
		this.transactional = transactional;
	}	
}
