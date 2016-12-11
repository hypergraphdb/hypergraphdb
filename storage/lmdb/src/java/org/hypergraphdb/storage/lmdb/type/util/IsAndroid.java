package org.hypergraphdb.storage.lmdb.type.util;

/**
 * Some miscellaneous utility functions.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class IsAndroid
{
	public static final boolean isAndroid = isAndroid();

	static boolean isAndroid()
	{
		try
		{
			Class.forName("android.os.Process");
			return true;
		}
		catch (Throwable ignored)
		{
			return false;
		}
	}
}
