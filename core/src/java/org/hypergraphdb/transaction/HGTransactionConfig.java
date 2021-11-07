package org.hypergraphdb.transaction;

/**
 * 
 * <p>
 * Encapsulates configuration parameters for a single transaction. 
 * </p>
 * 
 * @author Borislav Iordanov
 *
 */
public class HGTransactionConfig
{
    public static final HGTransactionConfig DEFAULT = new HGTransactionConfig();
    public static final HGTransactionConfig NO_STORAGE = new HGTransactionConfig();
    public static final HGTransactionConfig READONLY = new HGTransactionConfig();
    public static final HGTransactionConfig WRITE_UPGRADABLE = new HGTransactionConfig();
    
    static
    {
        NO_STORAGE.setNoStorage(true);
        READONLY.setReadonly(true);
        WRITE_UPGRADABLE.setWriteUpgradable(true);
    }
    
    private boolean noStorage = false;
    private boolean readonly = false;
    private boolean writeUpgradable = false;
    private Runnable beforeConflictRetry = null;

    public boolean isNoStorage()
    {
        return noStorage;
    }

    public HGTransactionConfig setNoStorage(boolean noStorage)
    {
        this.noStorage = noStorage;
        return this;
    }

    public boolean isReadonly()
    {
        return readonly;
    }

    public HGTransactionConfig setReadonly(boolean readonly)
    {
        this.readonly = readonly;
        writeUpgradable = false;
        return this;
    }

	public boolean isWriteUpgradable() 
	{
		return writeUpgradable;
	}

	public HGTransactionConfig setWriteUpgradable(boolean writeUpgradable) 
	{
		this.writeUpgradable = writeUpgradable;
		if (writeUpgradable)
		    readonly = true;
        return this;
	}

    public Runnable getBeforeConflictRetry() 
    {
        return beforeConflictRetry;
    }

    public HGTransactionConfig setBeforeConflictRetry(Runnable beforeConflictRetry) 
    {
        this.beforeConflictRetry = beforeConflictRetry;
        return this;
    }    
}