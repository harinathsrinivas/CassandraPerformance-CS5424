import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;

public abstract class Transaction {
	protected Session session;
	protected ConsistencyLevel writeConsistencyLevel;

	public Transaction(Session session, ConsistencyLevel writeConsistencyLevel) {
		this.session = session;
		this.writeConsistencyLevel = writeConsistencyLevel;
	}
	
	protected Session getSession() {
		return session;
	}

	public abstract void process(String[] args);
}
