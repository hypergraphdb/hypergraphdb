package org.hypergraphdb.query;

import java.util.regex.Pattern;

import org.hypergraphdb.util.HGUtils;

/**
 * Base class for matching string values using a regular expression.
 * 
 * @author Niels Beekman
 */
public abstract class AtomRegExPredicate implements HGQueryCondition, HGAtomPredicate
{
	private Pattern pattern;
	
	public AtomRegExPredicate()
	{
	}

	public AtomRegExPredicate(Pattern pattern)
	{
		this.pattern = pattern;
	}

	public Pattern getPattern()
	{
		return pattern;
	}
	
	public void setPattern(Pattern pattern)
	{
		this.pattern = pattern;
	}
	
	protected boolean satisfies(Object value)
	{
		if (pattern == null)
			throw new IllegalStateException("No regular expression pattern provided");
		if (value == null)
			return false;
		return pattern.matcher(value.toString()).matches();
	}
	
	public int hashCode() 
	{ 
		if (pattern == null)
			return 0;
		return HGUtils.hashThem(pattern.pattern(), pattern.flags());
	}
	
	public boolean equals(Object x)
	{
		if (! (x instanceof AtomRegExPredicate))
			return false;
		else
		{
			AtomRegExPredicate c = (AtomRegExPredicate)x;
			if (pattern == c.pattern)
				return true;
			if (pattern == null)
				return false;
			if (c.pattern == null)
				return false;
			return pattern.pattern().equals(c.pattern.pattern()) && pattern.flags() == c.pattern.flags();
		}
	}	
}
