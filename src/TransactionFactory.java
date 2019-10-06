import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;

public class TransactionFactory {
	private Session session;
	private ConsistencyLevel writeConsistencyLevel;
	
	public TransactionFactory(Session session, ConsistencyLevel writeConsistencyLevel) {
		this.session = session;
		this.writeConsistencyLevel = writeConsistencyLevel;
	}
	
	public Transaction getTransaction(String type) {
		switch (type) {
			case "N":
				return new NewOrder(session, writeConsistencyLevel);
			case "P":
				return new Payment(session, writeConsistencyLevel);
			case "D":
				return new Delivery(session, writeConsistencyLevel);
			case "O":
				return new OrderStatus(session, writeConsistencyLevel);
			case "S":
				return new StockLevel(session, writeConsistencyLevel);
			case "I":
				return new PopularItem(session, writeConsistencyLevel);
			case "T":
				return new TopBalance(session, writeConsistencyLevel);
			case "R":
				return new RelatedCustomer(session, writeConsistencyLevel);
			default:
				throw new IllegalArgumentException("invalid transaction type '" + type + "'");
		}
	}
}
