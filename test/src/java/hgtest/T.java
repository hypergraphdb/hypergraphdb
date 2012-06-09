package hgtest;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.logging.FileHandler;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.util.HGUtils;

@SuppressWarnings("unchecked")
public class T
{
	public static void sleep(long millis)
	{
		try { Thread.sleep(millis) ; }
		catch (InterruptedException ex) { }
	}
	
    public static String getTmpDirectory()
    {
        return System.getProperty("user.home") + File.separator + "hgtest.tmp";
    }

    /**
     * Return a random integer between 0 (inclusive) and i (exclusive).
     * 
     * @param i
     * @return
     */
    public static int random(int i)
    {
        return random(0, i);
    }

    /**
     * Return a random integer between i (inclusive) and j (exclusive).
     * 
     * @param i
     * @return
     */
    public static int random(int i, int j)
    {
        return i + (int) (Math.random() * (j - i));
    }

    public static void swap(Object[] A, int i, int j)
    {
        Object x = A[i];
        A[i] = A[j];
        A[j] = x;
    }

    public static void swap(List L, int i, int j)
    {
        Object x = L.get(i);
        L.set(i, L.get(j));
        L.set(j, x);
    }

    public static void shuffle(List L)
    {
        for (int i = 0; i < L.size(); i++)
            swap(L, random(i), random(i, L.size()));
    }

    /**
     * Move the result set forward maxSteps if possible. Return the number of
     * successful forward moves.
     */
    public static int forward(HGSearchResult<?> rs, int maxSteps)
    {
        int i = 0;
        for (; i < maxSteps && rs.hasNext(); i++)
            rs.next();
        return i;
    }

    /**
     * Move the result set forward maxSteps if possible. Return the number of
     * successful forward moves.
     */
    public static int back(HGSearchResult<?> rs, int maxSteps)
    {
        int i = 0;
        for (; i < maxSteps && rs.hasPrev(); i++)
            rs.prev();
        return i;
    }

    /**
     * Go back and forth on a result set an 'iteration' number of times. Assume
     * the result set is not empty and it already has a current position. The
     * windowSize parameters controls how far we are going to move - a random
     * number of steps b/w 0 and windowSize is actually used.
     */
    public static void backAndForth(HGSearchResult<?> rs, int windowSize,
            int iteration)
    {
        boolean advance = true;
        for (int i = 0; i < iteration; i++)
        {
            Object x = rs.current();
            int steps = random(windowSize);
            steps = forward(rs, steps);
            if (back(rs, steps) != steps)
                throw new RuntimeException("Moved " + steps
                        + " forward, but not backward.");
            if (!x.equals(rs.current()))
                throw new RuntimeException("Moving " + steps
                        + " steps forward and backward missed current " + x);
            if (advance)
            {
                forward(rs, random(windowSize));
                if (!rs.hasNext())
                {
                    back(rs, random(windowSize));
                    advance = false;
                }
            }
            else
            {
                back(rs, random(windowSize));
                if (!rs.hasPrev())
                {
                    forward(rs, random(windowSize));
                    advance = true;
                }
            }

        }
    }

    public static String getResourceContents(String resource)
    {
        StringBuilder contents = new StringBuilder();
        InputStream in = HGUtils.class.getResourceAsStream(resource);
        BufferedReader input = new BufferedReader(new InputStreamReader(in));
        try
        {
            String line = null;
            while ((line = input.readLine()) != null)
            {
                contents.append(line);
                contents.append(System.getProperty("line.separator"));
            }
        }
        catch (IOException ex)
        {
            throw new RuntimeException(ex);
        }
        finally
        {
            try
            {
                in.close();
            }
            catch (Throwable t)
            {
            }
        }
        return contents.toString();
    }

    public static Logger getLogger(String name)
    {
        Logger logger = Logger.getLogger(name);
        if (logger.getHandlers().length != 0)
            return logger;
        FileHandler fh;
        try
        {
            fh = new FileHandler(getTmpDirectory() + File.separator + name + ".log", true);
            logger.addHandler(fh);
            logger.setLevel(Level.ALL);
            fh.setFormatter(new Formatter() {
                public String format(LogRecord record)
                {
                    return record.getMessage() + "\n";
                }            
            });
        }
        catch (SecurityException e)
        {
            e.printStackTrace();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        return logger;        
    }
    
    public static void writeToFile(String filename, String data)
    {
    	try
    	{
    		FileWriter out = new FileWriter(filename);
    		out.write(data);
    		out.close();
    	}
    	catch (Exception ex)
    	{
    		throw new RuntimeException(ex);
    	}
    }
    
    public static String readFromFile(String filename)
    {
    	try
    	{
    		StringBuilder sb = new StringBuilder();
    		FileReader in = new FileReader(filename);
    		char [] buf = new char[1024];
    		for (int c = in.read(buf); c > 0; c = in.read(buf))
    			sb.append(buf, 0, c);
    		in.close();
    		return sb.toString();
    	}
    	catch (Exception ex)
    	{
    		throw new RuntimeException(ex);
    	}
    }    	
}
