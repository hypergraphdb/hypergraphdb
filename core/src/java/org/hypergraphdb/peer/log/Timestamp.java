package org.hypergraphdb.peer.log;

import org.hypergraphdb.peer.StorageService;

/**
 * @author ciprian.costa
 * Ensures ordering on events originated for the current peer.
 */
public class Timestamp implements Comparable<Timestamp>
{
	private int counter;

	public Timestamp()
	{
		counter = 0;
	}

	public Timestamp(int counter)
	{
		this.counter = counter;
	}

	public Timestamp moveNext()
	{
		Timestamp result = new Timestamp(counter);

		counter++;
		return result;
	}
	
	public String toString()
	{
		return "time = " + ((Integer)counter).toString();
	}

	public int getCounter()
	{
		return counter;
	}

	public void setCounter(int counter)
	{
		this.counter = counter;
	}
	
	public Timestamp clone()
	{
		return new Timestamp(counter);
	}

	public int compareTo(Timestamp o)
	{
		if ((o == null) || (counter > o.counter)) return 1;
		else if (counter == o.counter) return 0;
		else return -1;
	}
	
}
