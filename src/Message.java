import java.io.Serializable;







public class Message implements Serializable,Comparable<Message> {
	MessageType mt; 
	int receiver;
	int sender;
	int reqID;
	long timeStamp;
	public Message(int receiver, int sender ,MessageType mt, long timeStamp, int reqID) {
		this.receiver = receiver;
		this.sender = sender;
		this.mt=mt;
		this.timeStamp = timeStamp;
		this.reqID =reqID;
	}
	public int compareTo(Message o) {
		if (this.timeStamp > o.timeStamp) {
			return 1;
		} else if (this.timeStamp < o.timeStamp) {
			return -1;
		} else {
			return 0;
		}
	}
	
	public boolean equals(Object obj) {
		Message m;
		if (!(obj instanceof Message)) {
			return false;
		} else {
			m = (Message) obj;
		}
		return m.mt.equals(mt) && m.sender == sender;
	}

}
