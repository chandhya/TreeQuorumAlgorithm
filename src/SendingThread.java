import java.io.IOException;
import java.io.ObjectOutputStream;


public class SendingThread extends Thread {
	int receiver;
	MessageType mt;
	long timeStamp;
	ObjectOutputStream oos;
	SendingThread(int receiver,MessageType mt, long timeStamp){
		this.receiver = receiver;
		this.mt = mt;
		this.timeStamp = timeStamp;
	}
	@Override
	public void run(){
		if(mt.equals(MessageType.INITIATE)){
			Message m1 = new Message(receiver,Node.nodeNo,MessageType.INITIATE,timeStamp,-1);
			oos = Node.outMap.get(receiver);
			try {
				oos.writeObject(m1);
				oos.flush();
			} catch (IOException e) {
				
				e.printStackTrace();
			}
			System.out.println("Node "+Node.nodeNo+" sending initiate to "+receiver+" at time "+timeStamp+" with req ID "+Node.reqID);
		}
		else if(mt.equals(MessageType.REQUEST)){
			Message m1 = new Message(receiver,Node.nodeNo,MessageType.REQUEST,timeStamp,Node.reqID);
			oos = Node.outMap.get(receiver);
			synchronized(oos){
				try {
					oos.writeObject(m1);
					oos.flush();
				}
				catch (IOException e) {
					e.printStackTrace();
				}
				System.out.println("Node "+Node.nodeNo+" sending request to "+receiver+" at time "+timeStamp+" with req ID "+Node.reqID);
				Node.incrementSentMsgCount();
				Node.totalSentCount++;
				long[] cache = Node.timeLog.get(Node.reqID);
				cache[3] = Node.getSentMsgCount();
				Node.timeLog.put(Node.reqID, cache);
				
			}
		}
		else if(mt.equals(MessageType.GRANT)) {
			Message m1 = new Message(receiver,Node.nodeNo,MessageType.GRANT,timeStamp,-1);
			Node.tokens.put(m1.receiver, 0);

			oos = Node.outMap.get(receiver);
			synchronized(oos){
			try {
				oos.writeObject(m1);
				oos.flush();
			}
			catch (IOException e) {
				e.printStackTrace();
			}
		}
			System.out.println("Node "+Node.nodeNo+" sending grant to "+receiver+" at time "+timeStamp);
			Node.totalSentCount++;
			
		}
		else if(mt.equals(MessageType.RELEASE)) {
			System.out.println("Node "+Node.nodeNo+" sending release to "+receiver+" at time "+timeStamp);
			Node.totalSentCount++;
			/*Node.incrementSentMsgCount();
			long[] cache = Node.timeLog.get(Node.reqID);
			cache[3] = Node.getSentMsgCount();
			Node.timeLog.put(Node.reqID, cache);
			//Node.sentMsgCount=0;*/
			
			Message m1 = new Message(receiver,Node.nodeNo,MessageType.RELEASE,timeStamp,-1);
			Node.tokens.put(m1.receiver, 0);
			oos = Node.outMap.get(receiver);
			synchronized(oos){
			try {
				oos.writeObject(m1);
				oos.flush();
			}
			catch (IOException e) {
				e.printStackTrace();
			}
		}
			
			
		}
		else if(mt.equals(MessageType.COMPLETE)){
			
			Message m1 = new Message(receiver,Node.nodeNo,MessageType.COMPLETE,timeStamp,-1);
			System.out.println("Node "+Node.nodeNo+" sending complete message to "+receiver+" at time "+timeStamp);
			oos = Node.outMap.get(receiver);
			
			try {
				oos.writeObject(m1);
				oos.flush();
			} catch (IOException e) {
				
				e.printStackTrace();
				
			}
		}
		else if(mt.equals(MessageType.NOTIFY)){
			
			Message m1 = new Message(receiver,Node.nodeNo,MessageType.NOTIFY,timeStamp,-1);
			System.out.println("Node "+Node.nodeNo+" sending final notification to "+receiver+" at time "+timeStamp);
			oos = Node.outMap.get(receiver);
			
			try {
				oos.writeObject(m1);
				oos.flush();
			} catch (IOException e) {
				
				e.printStackTrace();
				
			}
		}

		
	}
}
