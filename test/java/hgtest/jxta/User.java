package hgtest.jxta;

public class User
{
	private int userId;
	private String name;
	
	public User()
	{
		
	}
	public User(int userId, String name)
	{
		this.userId = userId;
		this.name = name;
	}
	
	public int getUserId()
	{
		return userId;
	}
	public void setUserId(int userId)
	{
		this.userId = userId;
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
		return "User(id=" + userId + ", name=" + name + ")";
	}
	
	public String getPart()
	{
		return Integer.toString(userId).substring(0, 1);
	}
	public void setPart(String part)
	{
		
	}
}
