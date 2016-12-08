import java.util.TimerTask;

public class ClockTick extends TimerTask {

	@Override
	public void run() {
		Node.tick();

	}

}
