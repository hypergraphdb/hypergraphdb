package hgtest.beans;

/**
 * <p>
 * A test bean for HyperGraph's correct management of recursively defined types.
 * </p>
 * 
 * @author Borislav Iordanov
 *
 */
public class CircRefBean 
{
	private CircRefBean ref1;
	private CircRefBean ref2;
	private CircRefOtherBean other;
	
	public CircRefOtherBean getOther() {
		return other;
	}
	public void setOther(CircRefOtherBean other) {
		this.other = other;
	}
	public CircRefBean getRef1() {
		return ref1;
	}
	public void setRef1(CircRefBean ref1) {
		this.ref1 = ref1;
	}
	public CircRefBean getRef2() {
		return ref2;
	}
	public void setRef2(CircRefBean ref2) {
		this.ref2 = ref2;
	}
	
	public static CircRefBean make()
	{
		CircRefBean x = new CircRefBean();
		x.setRef1(x);
		
		CircRefOtherBean other = new CircRefOtherBean();		
		
		x.setOther(other);
		
		CircRefDerived y = new CircRefDerived();
		CircRefOtherBean other2 = new CircRefOtherBean();
		other2.setRef(x);
		other.setRef(y);
		y.setOther(other2); 
		y.setRef1(x);
		y.setRef2(y);		
		x.setRef2(y);
		
		return x;
	}
}