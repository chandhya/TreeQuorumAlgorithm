import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.Timer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.PriorityBlockingQueue;
public class Node  {
	//numeric constants
		public static final int SERVER_PORT = 1926;
		public static final int serverStartNode=1;
		public static final int serverEndNode=7;
		public static final int clientStartNode=8;
		public static final int clientEndNode=13;
		public static final int serverNodes=7;
		public static final int clientNodes=5;
		public static final int totalCSCount=19;
		public static final int timeUnit=10;
		public static int startRange1=5;
		public static int endRange1=10;
		public static long timeinCS=3;
		public static long time;
		static Timer clock;
		static Timer waitClock;
		//Connectivity related variables
		static InetSocketAddress currentNode;
		public static int nodeNo;
		public static int index=0;
		public static String[] serverNodeNos= new String[serverNodes];
		public static String[] nodeNos= new String[clientEndNode-1];
		public static volatile int reqID;
		
		//Variables that record the count of replies, requests and msgs exchanged
		public static int repCount;
		public static int currentCSCount;
		public static volatile int completeCount;
		public static volatile int sentMsgCount;
		public static volatile int receivedMsgCount;
		public static volatile int totalSentCount;
		public static volatile int totalReceivedCount;
		public static volatile int initiateMsgCount;
		
		//Boolean flags that is used to notify state of the node i.e(In critical section, if it has completed etc) 
		public static volatile boolean isClient = false;
		public static volatile boolean timeFlag =true;
		public static volatile boolean canIStart=false;
		public static volatile boolean isComplete=false;
		public static volatile boolean isinCS=false;
		public static volatile boolean notified=false;
		public static volatile boolean isReady =false;
		public static volatile boolean isLocked =false;
		public static volatile boolean hasExited =false;
		public static volatile boolean canSendNotify = false;
		//File write related variables
		public static PrintWriter out;
		public static BufferedWriter bfw;
		static String FILE_NAME;
		static String FILE_NAME_AGRR = "Aggregate_Report.txt";
		static File fileAggr = new File(FILE_NAME_AGRR);
			
		//Collections used to ensure concurrency during thread operations
		public static ConcurrentHashMap<Integer, ObjectOutputStream> outMap = new ConcurrentHashMap<Integer, ObjectOutputStream>();
		//Comparator anonymous class implementation
	    /*public static Comparator<Message> tsComparator = new Comparator<Message>(){
	        public int compare(Message m1, Message m2) {
	            return (int) (m1.timeStamp - m2.timeStamp);
	        }
	    };*/
		public static Queue<Message> reqQueue = new PriorityBlockingQueue<Message>();
		public static ConcurrentHashMap<Integer,long[]> timeLog = new ConcurrentHashMap<Integer,long[]>();
		public static ConcurrentHashMap<Integer,Integer> tokens = new ConcurrentHashMap<Integer,Integer>();
		public static CopyOnWriteArrayList<Socket> connectedNodes = new CopyOnWriteArrayList<Socket>();
		public static CopyOnWriteArrayList<Integer> grants = new CopyOnWriteArrayList<Integer>();
		public static CopyOnWriteArrayList<ListeningThread> threadListeners = new CopyOnWriteArrayList<ListeningThread>();
		static List<ArrayList<Integer>> quorumSets = new ArrayList<ArrayList<Integer>>();

public static void main(String args[]){
	try {
		currentNode = new InetSocketAddress(InetAddress.getLocalHost().getHostName(),SERVER_PORT);
		nodeNo = Integer.parseInt((currentNode.getHostName().substring(2,4)));
		System.out.println("Node no is - "+nodeNo);
		FILE_NAME = "node_"+nodeNo+".txt";
		File file = new File (FILE_NAME);
		out = new PrintWriter(file);
		if(!fileAggr.exists()){
			fileAggr.createNewFile();
		}
		if(nodeNo>=clientStartNode)
			isClient =true;
		initializeNetwork();
		startWork();
		logData();
		closeSockets();
	} catch (UnknownHostException e) {
		
		e.printStackTrace();
	} catch (FileNotFoundException e) {
		
		e.printStackTrace();
	} catch (IOException e) {
		
		e.printStackTrace();
	}
}

private static void closeSockets() {
	for(Socket socket:Node.connectedNodes)
		try {
			socket.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
}

private static void initializeNetwork() {
	if(isClient){
		BTree bt = constructTree();
		constructQuorum(bt);
		Thread client = new Client();
		client.start();
		index=0;
		for(int i=serverStartNode;i<=serverEndNode;i++){
				if(i<10)
					serverNodeNos[index] = "0"+String.valueOf(i);
				else
					serverNodeNos[index] = ""+String.valueOf(i);
				index++;
		}
		for(int z=0;z<serverNodeNos.length;z++)
			System.out.println("Server Node no "+serverNodeNos[z]);
	}
	else{
		Thread server = new Server();
		server.start();
		System.out.println("Server started");
	}
}


private static BTree constructTree() {
	BTree bt = new BTree();
	BTree.NodeB root = bt.new NodeB((Object)Integer.valueOf(1));
	bt.root = root;
	    BTree.NodeB two = bt.new NodeB((Object)Integer.valueOf(2));
	    BTree.NodeB three = bt.new NodeB((Object)Integer.valueOf(3));
	    BTree.NodeB four = bt.new NodeB((Object)Integer.valueOf(4));
	    BTree.NodeB five = bt.new NodeB((Object)Integer.valueOf(5));
	    BTree.NodeB six = bt.new NodeB((Object)Integer.valueOf(6));
	    BTree.NodeB seven = bt.new NodeB((Object)Integer.valueOf(7));
	    bt.root.left = two;
	    bt.root.right = three;
	    two.left = four;
	    two.right = five;
	    three.left = six;
	    three.right = seven;
	    return bt;
}
private static void constructQuorum(BTree bt) {
	generateQuorumSets(bt.root,new ArrayList<Integer>());
	ArrayList<Integer> listCache;
	listCache= new ArrayList<Integer>(Arrays.asList(1,4,5));
	quorumSets.add(listCache);
	listCache= new ArrayList<Integer>(Arrays.asList(1,6,7));
	quorumSets.add(listCache);
	listCache= new ArrayList<Integer>(Arrays.asList(2,3,4,6));
	quorumSets.add(listCache);
	listCache= new ArrayList<Integer>(Arrays.asList(2,3,4,7));
	quorumSets.add(listCache);
	listCache= new ArrayList<Integer>(Arrays.asList(2,3,5,6));
	quorumSets.add(listCache);
	listCache= new ArrayList<Integer>(Arrays.asList(2,3,4,7));
	quorumSets.add(listCache);
	listCache= new ArrayList<Integer>(Arrays.asList(3,4,5,6));
	quorumSets.add(listCache);
	listCache= new ArrayList<Integer>(Arrays.asList(3,4,5,7));
	quorumSets.add(listCache);
	listCache= new ArrayList<Integer>(Arrays.asList(2,4,6,7));
	quorumSets.add(listCache);
	listCache= new ArrayList<Integer>(Arrays.asList(2,5,6,7));
	quorumSets.add(listCache);
	listCache= new ArrayList<Integer>(Arrays.asList(4,5,6,7));
	quorumSets.add(listCache);
}
public static void generateQuorumSets(BTree.NodeB root, ArrayList<Integer> list) {
    if(root == null) {
        return;
    }
    list.add((Integer) root.value);
    if(root.left == null && root.right == null) {
    	ArrayList<Integer> quorumSet = new ArrayList<Integer>(list);
    	quorumSets.add(quorumSet);
        System.out.println(list);
        list.remove((Integer)root.value);
        return;
    }
    generateQuorumSets(root.left, list);
    generateQuorumSets(root.right, list);
    // cast so we don't remove by index  (would happen if 'data' is an int)
    list.remove((Integer)root.value);
}
private static void startWork() {
	//SendingThread st;
			while(true){
				if(isReady)
					break;
			}
			for(int k=0;k<Node.threadListeners.size();k++){
				ListeningThread lt = threadListeners.get(k);
				lt.start();
			}
			System.out.println("Initializing network..");
			if(isClient){
			for(int j=0;j<Node.serverNodeNos.length;j++) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				long timeStamp = Node.getTime();
				int receiver = Integer.parseInt(Node.serverNodeNos[j]);
				if(nodeNo !=receiver){
				SendingThread st = new SendingThread(receiver,MessageType.INITIATE,timeStamp);
				st.start();
				}
			}
			}
			else {
				for(int j=clientStartNode;j<=Node.clientEndNode;j++) {
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					long timeStamp = Node.getTime();
					int receiver = j;
					if(nodeNo !=receiver && j!=10){
					SendingThread st = new SendingThread(receiver,MessageType.INITIATE,timeStamp);
					st.start();
					}
			}
			}
			while(true){
				if(canIStart)
				break;
			}
			clock = new Timer();
			clock.schedule(new ClockTick(), 0, 1);
			if(isClient){
			for(int i=0;i<totalCSCount;i++) {
				Random random = new Random();
				int randInt = getRandomInteger(startRange1,endRange1,random);
				try {
					Thread.sleep(randInt*timeUnit);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				
				Node.sentMsgCount=0;
				Node.grants.clear();
				Node.setReceivedMsgCountToZero();
				//long timeStamp = Node.getTime();
				Node.tick();
				long timeStamp = Node.getTime();
				long[] timeElapsed = new long[5];
				timeElapsed[0] = timeStamp; 
				timeElapsed[1] = 1;
				isinCS =false;
				Node.reqID++;
				Node.timeLog.put(Node.reqID, timeElapsed);
				for(int j=0;j<Node.serverNodeNos.length;j++) {
				int receiver = Integer.parseInt(Node.serverNodeNos[j]);
				if(nodeNo != receiver && !Node.isinCS){
				SendingThread st = new SendingThread(receiver,MessageType.REQUEST,timeStamp);
				st.start();
				}
			}
				long currentTime = Node.getTime();
			while(true){
							
				if(hasExited)
				{
					hasExited =false;
					break;
				}
				if(Node.getTime() - currentTime>10000){
					System.out.println("Deadlock occured!");
					break;
				}
				
			}
		}
			while(true){
				if(getCSCount()==totalCSCount)
				{
				try {
					Thread.sleep(2000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				isComplete =true;
				
				sendCompleteMsgs();
				break;
				}
			}
			}
			if(nodeNo == serverStartNode)
			while(true){
				if(canSendNotify) {
			for(int k=clientStartNode;k<=clientEndNode;k++){
				int receiver = k;
				if(receiver != 10){
					System.out.println("Now sending notification");
					//long timeStamp = Node.getTime();
					Node.tick();
					long timeStamp2 = Node.getTime();
					SendingThread st2 = new SendingThread(receiver,MessageType.NOTIFY,timeStamp2);
					st2.start();
					try {
						Thread.sleep(2000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					Node.notified=true;
				}
			}
			break;
			}
		}
			while(true){
				if(notified){
				try {
					Thread.sleep(2000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				break;
				}
			}
			clock.cancel();
		}
private static void sendCompleteMsgs() {
	SendingThread st2;
		Node.tick();
		long timeStamp = Node.getTime();
		st2 = new SendingThread(serverStartNode,MessageType.COMPLETE,timeStamp);
		st2.start();
	
}

//Method that handles printing output
	private static void logData() {
		System.out.println("DATA COLLECTION:");
		out.println("DATA COLLECTION:");
		if(isClient){
		String format = "%-20s%-10s%-10s%n";
		System.out.printf(format, "CS entry","Latency","Msgs exchanged(Req sent+reply received)");
		System.out.println();
		out.printf(format, "CS entry","Latency","Msgs exchanged(Req sent+reply received)");
		out.println();
		}
		int msgsSent=0 ,msgsReceived=0;
		List<Integer> reqIds = new ArrayList<Integer>();
		for(Integer id :timeLog.keySet())
			reqIds.add(id);
		
		Collections.sort(reqIds);
		//for(Integer r:timeLog.keySet()){
		for(Integer r:reqIds){
			long[] cache = timeLog.get(r);
			msgsSent+=cache[3];
			msgsReceived+=cache[4];
			
			if(isClient){
			String format = "%-20s%-10s%-10s%n";
			System.out.printf(format, r, String.valueOf(cache[2]-cache[0]), String.valueOf(cache[3]+cache[4]));
			System.out.println();
			out.printf(format, r, String.valueOf(cache[2]-cache[0]), String.valueOf(cache[3]+cache[4]));
			}
			out.println();
		}
		System.out.println("Total Msgs sent :"+Node.totalSentCount);
		//System.out.println("\tTotal Requests sent :"+msgsSent);
		//System.out.println("\tTotal Replies sent :"+(Node.totalSentCount - msgsSent));
		System.out.println("Total Msgs received :"+Node.totalReceivedCount);
		//System.out.println("\tTotal Requests received :"+(Node.totalReceivedCount-msgsReceived));
		//System.out.println("\tTotal Replies received :"+msgsReceived);
		System.out.println("Total Msgs exchanged :"+(msgsReceived+msgsSent));
		out.println("Total Msgs sent :"+Node.totalSentCount);
		//out.println("\tTotal Requests sent :"+msgsSent);
		//out.println("\tTotal Replies sent :"+(Node.totalSentCount - msgsSent));
		out.println("Total Msgs received :"+Node.totalReceivedCount);
		//out.println("\tTotal Requests received :"+(Node.totalReceivedCount-msgsReceived));
		//out.println("\tTotal Replies received :"+msgsReceived);
		out.println("Total Msgs exchanged :"+(msgsReceived+msgsSent));
		Node.out.close();
	}

//Method that returns current system time/logical time
static long getTime() {
	if(timeFlag)
	return  System.currentTimeMillis();
	else
	return time;
}
//Synchronized methods to update values that are changed by multiple threads
	public synchronized static void tick() {
		time++;
	}
	
	 //Method to generate random numbers in a certain range - Source :http://www.javapractices.com/topic/TopicAction.do?Id=62
	  private static int getRandomInteger(int aStart, int aEnd, Random aRandom){
		    if (aStart > aEnd) {
		      throw new IllegalArgumentException("Start cannot exceed End.");
		    }
		    //get the range, casting to long to avoid overflow problems
		    long range = (long)aEnd - (long)aStart + 1;
		    // compute a fraction of the range, 0 <= frac < range
		    long fraction = (long)(range * aRandom.nextDouble());
		    int randomNumber =  (int)(fraction + aStart);    
		   return randomNumber;
		  }
	//Method that gets the node number based on node
	 static int getNodeNumber(Socket connectedNode) {
			if(connectedNode!=null)
			{
			int x = Integer.parseInt((connectedNode.getInetAddress().getHostName().substring(2,4)));
			return x;
			}
			return 0;
		}
	 
	 
	public static synchronized void sendGrant(Message m, boolean isInitialRequest) {
	
		Node.resetTime(m.timeStamp);
		if(isInitialRequest){
		if(!Node.isLocked){
			Node.isLocked =true;
			SendingThread st = new SendingThread(m.sender,MessageType.GRANT,Node.getTime());
			st.start();
		}
		else{
			reqQueue.add(m);
		}
	 }
	else{
		if(!reqQueue.isEmpty()){
			Message cache = reqQueue.remove();
			Node.isLocked =true;
			SendingThread st = new SendingThread(cache.sender,MessageType.GRANT,Node.getTime());
			st.start();
		}
		else{
			Node.isLocked =false;
		}
	}
  }
	public synchronized static void resetTime(long ts){
		Node.tick();
		Node.time = Node.time<ts?ts:Node.time;
		}

	public static synchronized boolean checkForQuorum() {
		for(int i=0;i<quorumSets.size();i++){
		//System.out.println("Checking for grants");
			ArrayList<Integer> tempGrants = new ArrayList<Integer>(grants);
			Collections.sort(tempGrants);
			//System.out.println(quorumSets.get(i));
			//System.out.println(tempGrants);
			if(quorumSets.get(i).equals(tempGrants) ||tempGrants.containsAll(quorumSets.get(i))){
				
				return true;
			}
		}
		return false;
	}

	public static synchronized void enterCS() {
		isinCS =true; 
		currentCSCount++;
		System.out.println("Entering CS - Node "+nodeNo);
		out.println("Entering CS - Node "+nodeNo);
		FileWriter fw;
		try {
			fw = new FileWriter(fileAggr,true);
			fw.write("Entering CS - Node "+nodeNo+"\n");
			long timeStamp = Node.getTime();
			System.out.println("Current physical time: "+Node.getSysTime());
			out.println("Current physical time: "+Node.getSysTime());
			Thread.sleep(timeinCS*timeUnit);
			if(!timeLog.isEmpty()) {
				 for(Integer i:timeLog.keySet()) {
					 long[] cache =new long[5];
					 cache= timeLog.get(i);
					 	if(cache[1]==1)
						 {
					 	cache[1] =0; 
					 	cache[2] = timeStamp;
					 	timeLog.put(i, cache);
					 	break;
						 }
					 	else 
					 	 continue;
					 }
				 }
			long timeStampEnd = Node.getTime();
			System.out.println("Current physical time: "+Node.getSysTime());
			out.println("Current physical time: "+Node.getSysTime());
			fw.write("Current physical time: "+Node.getSysTime()+"\n");
			fw.close();
			sendRelease();
			hasExited =true;
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
	}

	private static long getSysTime() {
		return  System.currentTimeMillis();
		
	}

	private static void sendRelease() {
		Node.tick();
		Node.grants.clear();
		List<SendingThread> releases = new ArrayList<SendingThread>();
		for(int j=0;j<Node.serverNodeNos.length;j++) {
			int receiver = Integer.parseInt(Node.serverNodeNos[j]);
			if(nodeNo != receiver){
			SendingThread st = new SendingThread(receiver,MessageType.RELEASE,Node.getTime());
			releases.add(st);
			
			}
			}
		for(SendingThread stCur:releases)
			stCur.start();
		
		}
	static synchronized void setCSFlag(boolean flag){
		isinCS =flag;
	} 
	static synchronized boolean getCSFlag(){
		return isinCS;
		
	}
	static synchronized void incrementSentMsgCount(){
		sentMsgCount++;
	}
	static synchronized void incrementReceivedMsgCount(){
		receivedMsgCount++;
	}
	static synchronized void setReceivedMsgCountToZero(){
		receivedMsgCount = 0;
	}
	static synchronized void setSentMsgCountToZero(){
		sentMsgCount = 0;
	}
	static synchronized int getReceivedMsgCount(){
		return receivedMsgCount;
	}
	static synchronized int getSentMsgCount(){
		return sentMsgCount;
	}
	static synchronized int getCSCount(){
		return currentCSCount;
	}
}
