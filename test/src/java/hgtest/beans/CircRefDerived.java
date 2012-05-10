package hgtest.beans;

public class CircRefDerived extends CircRefBean 
{
	private CircRefOtherBean other2;

	public CircRefOtherBean getOther2() {
		return other2;
	}

	public void setOther2(CircRefOtherBean other2) {
		this.other2 = other2;
	}	
}