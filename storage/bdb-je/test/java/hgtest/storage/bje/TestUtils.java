package hgtest.storage.bje;

import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGRandomAccessResult;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author Yuriy Sechko
 */
public class TestUtils
{
	/**
	 * Deletes directory's content and then deletes directory itself. Deleting
	 * is not recursive.
	 * 
	 * @param directory
	 */
	public static void deleteDirectory(final File directory)
	{
		final File[] filesInTestDir = directory.listFiles();
		if (filesInTestDir != null)
		{
			for (final File eachFile : filesInTestDir)
			{
				eachFile.delete();
			}
		}
		directory.delete();
	}

	/**
	 * Iterates through result and copies encountered items to the list.
	 */
	public static <T> List<T> list(final HGRandomAccessResult<T> result)
	{
		final List<T> outputList = new ArrayList<T>();
		while (result.hasNext())
		{
			final T currentValue = result.next();
			outputList.add(currentValue);
		}
		return outputList;
	}

	/**
	 * Puts all handles which are accessible from given result set into hash
	 * set. In some test cases stored data returned as
	 * {@link HGRandomAccessResult}. Two results cannot be compared directly. So
	 * we put all handles into set and that compare two sets. The order of
	 * handles in result set (obtained from database) is difficult to predict.
	 */
	public static Set<HGPersistentHandle> set(
			final HGRandomAccessResult<HGPersistentHandle> handles)
	{
		final Set<HGPersistentHandle> allHandles = new HashSet<HGPersistentHandle>();
		while (handles.hasNext())
		{
			allHandles.add(handles.next());
		}
		return allHandles;
	}

	public static File createTempFile(final String prefix, final String suffix) {
		File tempFile;
		try {
			tempFile = File.createTempFile(prefix, suffix);
		} catch (IOException ioException) {
			throw new IllegalStateException(ioException);
		}
		return tempFile;
	}

	public static String getCanonicalPath(final File file) {
		String canonicalPath;
		try {
			canonicalPath = file.getCanonicalPath();
		} catch(IOException ioException) {
			throw new IllegalStateException(ioException);
		}
		return canonicalPath;
	}
}
