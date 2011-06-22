package org.hypergraphdb.query;

import java.util.regex.Pattern;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HyperGraph;

/**
 * A predicate that constrains the value of an atom using a regular expression.
 * 
 * @author Niels Beekman
 */
public class AtomValueRegExPredicate extends AtomRegExPredicate
{
	public AtomValueRegExPredicate()
	{
		super();
	}
	
	public AtomValueRegExPredicate(Pattern pattern)
	{
		super(pattern);
	}

	public boolean satisfies(HyperGraph hg, HGHandle handle)
	{
		Object atom = hg.get(handle);
		if (atom == null)
			return false;
		else
			return satisfies(atom);
	}
	
	public String toString()
	{
		StringBuilder result = new StringBuilder("regEx(");
		result.append(getPattern());
		result.append(")");
		return result.toString();
	}
}