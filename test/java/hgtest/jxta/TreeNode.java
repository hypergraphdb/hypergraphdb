package hgtest.jxta;

import java.util.HashSet;

public class TreeNode {
	private String value;
	private TreeNode left, right;
	
	public TreeNode(){
		
	}
	public TreeNode(String value, TreeNode left, TreeNode right){
		this.value = value;
		this.left = left;
		this.right = right;
	}
	
	public String toString(){
		return putInString(new HashSet<Object>());
	}		

	private String putInString(HashSet<Object> usedSet) {
		if (usedSet.contains(this)){
			return "(ref " + value + ")";
		}else{
			usedSet.add(this);
			
			return "(" + value + ((left == null) ? "(null)" : left.putInString(usedSet)) + ((right == null) ? "(null)" : right.putInString(usedSet)) + ")";
			
		}
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public TreeNode getLeft() {
		return left;
	}

	public void setLeft(TreeNode left) {
		this.left = left;
	}

	public TreeNode getRight() {
		return right;
	}

	public void setRight(TreeNode right) {
		this.right = right;
	}
	
	
}
