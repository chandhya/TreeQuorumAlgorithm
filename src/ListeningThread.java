import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;


public class ListeningThread extends Thread{
	Socket socket;
	ObjectOutputStream oos;
	ObjectInputStream iis;
	int curReqID;
	int nodeNo;
	public ListeningThread(Socket socket, ObjectInputStream iis)
	 {
		this.socket = socket;
		this.nodeNo = Node.getNodeNumber(socket);
		this.iis = iis;
		//System.out.println("I'm listening for messages from Node "+nodeNo);
	}
	@Override
	public void run(){
		while(true){
			Message m;
			synchronized (iis) {
			 try {
				m = (Message)iis.readObject();
				 if(m.mt.equals(MessageType.INITIATE)){
					 System.out.println("Initiate message from node "+m.sender);
					 Node.initiateMsgCount++;
					 if(Node.isClient){
						 System.out.println("Initiate message count"+Node.initiateMsgCount);
					 if(Node.initiateMsgCount==Node.serverNodes)
					 Node.canIStart=true;
					 }
					 else{
						if(Node.initiateMsgCount==Node.clientNodes)
						Node.canIStart=true; 
					 }
				 }
				 else if(m.mt.equals(MessageType.REQUEST)){
					
					System.out.println("Request received from node "+m.sender);
					Node.sendGrant(m,true);
					Node.totalReceivedCount++;
				 }
				 else if(m.mt.equals(MessageType.GRANT)){
					 Node.totalReceivedCount++;
					// if(!Node.isinCS){
					 if(curReqID<Node.reqID &&!Node.isinCS){
						 System.out.println("Grant received from node "+m.sender);
						// Node.out.println("Grant received from node "+m.sender);
						 Node.grants.add(m.sender);
						 Node.incrementReceivedMsgCount();
						 if(Node.checkForQuorum() && Node.getCSCount()<=Node.totalCSCount){
					 		curReqID = Node.reqID;
					 		long[] cache2 = Node.timeLog.get(Node.reqID);
					 		int temp = Node.getReceivedMsgCount();
							cache2[4] = temp;
							System.out.println("Received msgs Count"+Node.getReceivedMsgCount());
						  //  Node.out.println("Grant received from node "+m.sender);
							Node.timeLog.put(Node.reqID, cache2);
					 		Node.isinCS =true;
					 		Node.grants.clear();
					 		
					 		System.out.println("GRANTS NOW -"+Node.grants.size());
					 		Node.enterCS();
						 
					}
				 }
					
				
				 }
				 else if(m.mt.equals(MessageType.RELEASE)){
					 System.out.println("Release received from node "+m.sender);
					 for(int i = 0;i < Node.reqQueue.size();i++){
						 Message cache = Node.reqQueue.peek();
						 if(cache.sender == m.sender){
							 Node.reqQueue.remove(cache);
						 }
					 }
					 Node.sendGrant(m,false);
					 Node.totalReceivedCount++;
					
					 }
				 else if(m.mt.equals(MessageType.COMPLETE)){
						Node.completeCount++;
						//System.out.println("Complete count"+Node.completeCount);
						//System.out.println("Final Node - 1" +(Node.finalNodeNo-1));
						if(Node.completeCount==Node.clientNodes ) {
							//System.out.println("And it gets in");
							//synchronized(Node.notifyObject1){
								System.out.println("Received all completes -notifying!");
								Node.out.println("Received all completes -notifying!");
								Node.canSendNotify =true;
								//Node.notifyObject1.notify();
							//}
						}
					}
					else if(m.mt.equals(MessageType.NOTIFY)){
						if(Node.isClient){
							for(int i=0;i<Node.serverNodeNos.length;i++){
								Node.tick();
								long timeStamp2 = Node.getTime();
								int receiver = Integer.parseInt(Node.serverNodeNos[i]);
								SendingThread st2 = new SendingThread(receiver,MessageType.NOTIFY,timeStamp2);
								st2.start();
							}
						}
						Node.notified=true;
						Thread.sleep(2000);
						break;
					}
					if(Node.notified){
						Thread.sleep(2000);
						break;
					}
			} catch (ClassNotFoundException e) {
				//e.printStackTrace();
				break;
			} catch (IOException e) {
				//e.printStackTrace();
				break;
			} catch (InterruptedException e) {
				//e.printStackTrace();
				break;
			}
			}
		}
	}
}
