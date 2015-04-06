package hgtest.storage.bdb;

/**
 * This class introduced for loading native libraries.
 * <p>
 * As mentioned in {@link org.hypergraphdb.storage.bdb.BDBStorageImplementation}:
 * 
 * <pre>
 * This is solely because of failing to resolve dependencies under
 * Windows. For some reason, when libdb50 is loaded explicitly first,
 * it all works out fine.
 * </pre>
 * */
public class NativeLibrariesWorkaround
{
    /**
     * Hidden constructor.
     */
	private NativeLibrariesWorkaround()
	{
	}

	/**
	 * Loads {@code libdb53} then {@code libdb_java53} explicitly.
	 */
	public static void loadNativeLibraries()
	{
		load();
	}

	private static void load()
	{
		if (System.getProperty("os.name").toLowerCase().indexOf("win") > -1)
		{
			System.out.println("Force BerkleyDB DLL load order in tests.");
			System.loadLibrary("libdb53");
			System.loadLibrary("libdb_java53");
		}
	}
}
