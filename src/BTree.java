import java.util.ArrayList;
import java.util.HashMap;



public class BTree {
NodeB root=null;
HashMap<Integer,ArrayList<Node>> hm = new HashMap<Integer,ArrayList<Node>>();
class NodeB{
	NodeB left;
	NodeB right;
	Object value;
	NodeB(Object value){
		this.value=value;
	}
}
NodeB insert(NodeB root,NodeB node){
	if(root==null)
		root = new NodeB(node.value);
	else{
		if(((Integer)node.value).intValue()<((Integer)root.value).intValue())
			root.left=	insert(root.left,node);
		else
			root.right =insert(root.right,node);
	}
	return root;
}
public static void main(String args[]){
	
}
}