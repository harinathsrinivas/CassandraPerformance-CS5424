import java.util.List;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class OrderStatus extends Transaction {

	public OrderStatus(Session session, ConsistencyLevel writeConsistencyLevel) {
		super(session, writeConsistencyLevel);
	}

	@Override
	public void process(String[] args) {
		System.out.println("-------- Order-Status --------");
		int w_id = Integer.parseInt(args[1]);
		int d_id = Integer.parseInt(args[2]);
		int c_id = Integer.parseInt(args[3]);
		
		Row customer = getCustomer(w_id, d_id, c_id);
		Row order = getOrder(w_id, d_id, c_id);
		int o_id = order.getInt("o_id");
		List<Row> orderLines = getOrderLines(w_id, d_id, o_id);
		
		System.out.printf(
				"C_NAME: (%s, %s, %s), C_BALANCE: %f\n",
				customer.getString("c_first"), 
				customer.getString("c_middle"), 
				customer.getString("c_last"), 
				customer.getDecimal("c_balance")
		);
		System.out.printf(
				"O_ID: %d, O_ENTRY_D: %tc, O_CARRIER_ID: %d\n",
				order.getInt("o_id"), 
				order.getTimestamp("o_entry_d"),
				order.getInt("o_carrier_id")
		);
		for (Row orderLine: orderLines) {
			System.out.printf(
					"OL_I_ID: %d, OL_SUPPLY_W_ID: %d, OL_QUANTITY: %f " +
					"OL_AMOUNT: %f, OL_DELIVER_D: %tc\n",
					orderLine.getInt("ol_i_id"), 
					orderLine.getInt("ol_supply_w_id"),
					orderLine.getDecimal("ol_quantity"),
					orderLine.getDecimal("ol_amount"),
					orderLine.getTimestamp("ol_delivery_d")
			);
		}
	}

	private Row getCustomer(int w_id, int d_id, int c_id) {
		Session session = getSession();
		PreparedStatement ps = session.prepare(
			"SELECT c_first, c_middle, c_last, c_balance " + 
		    "FROM customers " + 
			"WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?;"
		);
		ResultSet resultSet = session.execute(ps.bind(w_id, d_id, c_id));
		List<Row> results = resultSet.all();
		
		if (results.isEmpty()) {
			throw new IllegalArgumentException("No matching customer");
		}
		return results.get(0);
	}
	
	private Row getOrder(int w_id, int d_id, int c_id) {
		Session session = getSession();
		PreparedStatement ps = session.prepare(
			"SELECT o_id, o_entry_d, o_carrier_id " +
		    "FROM ordersbyid " +
		    "WHERE o_w_id = ? AND o_d_id = ? AND o_c_id = ? " +
		    "ORDER BY o_id DESC " + 
		    "LIMIT 1 ALLOW FILTERING;"
		);
		ResultSet resultSet = session.execute(ps.bind(w_id, d_id, c_id));
		List<Row> results = resultSet.all();
		
		if (results.isEmpty()) {
			throw new IllegalArgumentException("No matching order");
		}
		return results.get(0);
	}
	
	private List<Row> getOrderLines(int w_id, int d_id, int o_id) {
		Session session = getSession();
		PreparedStatement ps = session.prepare(
			"SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d " +
		    "FROM orderlines " +
		    "WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ? " +
		    "ALLOW FILTERING;"
		);
		ResultSet resultSet = session.execute(ps.bind(w_id, d_id, o_id));
		List<Row> results = resultSet.all();
		return results;
	}
}
