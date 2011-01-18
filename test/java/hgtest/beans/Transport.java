package hgtest.beans;

import org.hypergraphdb.annotation.AtomReference;

public abstract class Transport
{
    @AtomReference("symbolic")
    private Person owner;

    public Person getOwner()
    {
        return owner;
    }

    public void setOwner(Person owner)
    {
        this.owner = owner;
    }
}