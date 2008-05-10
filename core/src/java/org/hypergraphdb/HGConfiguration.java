package org.hypergraphdb;

/**
 * 
 * <p>
 * A bean that holds configuration parameters for a HyperGraphDB initialization.
 * An instance can be passed to the {@link HGEnvironment#configure(String, HGConfiguration)} or 
 * {@link HGEnvironment#get(String, HGConfiguration)} methods.
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
	
	/**
	 * <p>
	 * <code>true</code> if the database is configured to support transactions and <code>false</code>
	 * otherwise.
	 * </p>
	 * 
	 * @return
	 */
	public boolean isTransactional()
	{
		return transactional;
	}

	/**
	 * <p>
	 * Specifies if the database should be opened in transactional mode which is the default 
	 * mode. Setting this flag to false should be done with care. It results in much faster 
	 * operations (4-5 times faster), but it can result in an unrecoverable crash. In general
	 * this should be used when a lot of data is being loaded into a brand new, or a properly
	 * backed up beforehand, database.
	 * </p>
	 * 
	 * @param transactional
	 */
	public void setTransactional(boolean transactional)
	{
		this.transactional = transactional;
	}	
}
