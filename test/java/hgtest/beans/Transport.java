package hgtest.beans;

import org.hypergraphdb.annotation.AtomReference;

public abstract class Transport
{
    @AtomReference("symbolic")
    private Person owner;
    @AtomReference("floating")
    private Float cost;
    @AtomReference("hard")
    private Integer age;

    
    public Person getOwner()
    {
        return owner;
    }

    public void setOwner(Person owner)
    {
        this.owner = owner;
    }

    public Float getCost()
    {
        return cost;
    }

    public void setCost(Float cost)
    {
        this.cost = cost;
    }

    public Integer getAge()
    {
        return age;
    }

    public void setAge(Integer age)
    {
        this.age = age;
    }
    
    
}