package hgtest.jxta;

public class SimpleBean {
	private String name;
	
	public SimpleBean()
	{
		
	}
	public SimpleBean(String name)
	{
		this.name = name;
	}

	public String getName() 
	{
		return name;
	}

	public void setName(String name) 
	{
		this.name = name;
	}
	
	public String toString()
	{
		return "SimpleBean: name = " + name;
	}
}
