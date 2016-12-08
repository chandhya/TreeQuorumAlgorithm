import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;


public class Server extends Thread {
	Socket socket;
    ObjectOutputStream oos;
	@Override
	 public void run(){
	 ServerSocket serverSocket=null;
		try {
			  serverSocket = new ServerSocket(Node.SERVER_PORT);
			  int count;
				for(count=0;count<Node.clientNodes;count++) {
					socket = serverSocket.accept();
						
					oos = new ObjectOutputStream(socket.getOutputStream());
					Node.outMap.put(Node.getNodeNumber(socket),oos);
					System.out.println("Accepted Connection from :"+Node.getNodeNumber(socket));
					ObjectInputStream iis = new ObjectInputStream(socket.getInputStream());
					ListeningThread lt = new ListeningThread(socket,iis);
					Node.threadListeners.add(lt);
					Node.connectedNodes.add(socket);
				}
				System.out.println("No of connections accepted at Server :"+count);
				Node.isReady =true;
				while(true){
					if(Node.notified)
					{
						System.out.println("Server socket to be closed!");
						serverSocket.close();
						break;
					}
				}
			}
			catch (IOException e) {
				e.printStackTrace();
			}
		}
	 }
