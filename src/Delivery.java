import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.stream.IntStream;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class Delivery extends Transaction {

	public Delivery(Session session, ConsistencyLevel writeConsistencyLevel) {
		super(session, writeConsistencyLevel);
	}

	@Override
	public void process(String[] args) {
		System.out.println("-------- Delivery --------");
		int w_id = Integer.parseInt(args[1]);
		int o_carrier_id = Integer.parseInt(args[2]);
		for (int d_id = 1; d_id <= 10; d_id++) {
			Row order;
			ResultSet update;
			int o_id;
			int o_ol_cnt;
			do {
				order = getOrder(w_id, d_id);
				if (order == null) break;
				o_id = order.getInt("o_id");
				update = updateOrder(w_id, d_id, o_id, o_carrier_id);
			} while (!update.wasApplied());
			if (order == null) continue;  // all orders are delivered
			o_ol_cnt = order.getDecimal("o_ol_cnt").intValue();
			o_id = order.getInt("o_id");
			updateOrderLines(w_id, d_id, o_id, o_ol_cnt);
			int c_id = order.getInt("o_c_id");
			BigDecimal totalAmount = getTotalAmount(w_id, d_id, o_id);
			Row customer = getCustomer(w_id, d_id, c_id);
			BigDecimal c_balance = customer.getDecimal("c_balance").add(totalAmount);
			int c_delivery_cnt = customer.getInt("c_delivery_cnt") + 1;
			updateCustomer(w_id, d_id, c_id, c_balance, c_delivery_cnt);
		}
	}

	private Row getOrder(int w_id, int d_id) {
		Session session = getSession();
		PreparedStatement ps = session.prepare(
			"SELECT o_id, o_c_id, o_ol_cnt " +
		    "FROM orders " +
		    "WHERE o_w_id = ? AND o_d_id = ? AND o_carrier_id = -1 " +
		    "ORDER BY o_id ASC " + 
		    "LIMIT 1 ALLOW FILTERING;"
		);
		ResultSet resultSet = session.execute(ps.bind(w_id, d_id));
		List<Row> results = resultSet.all();
		
		if (results.isEmpty()) {
			return null;
		}
		return results.get(0);
	}
	
	private BigDecimal getTotalAmount(int w_id, int d_id, int o_id) {
		Session session = getSession();
		PreparedStatement ps = session.prepare(
			"SELECT ol_amount " +
		    "FROM orderlines " +
		    "WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ? " +
		    "ALLOW FILTERING;"
		);
		ResultSet resultSet = session.execute(ps.bind(w_id, d_id, o_id));
		List<Row> results = resultSet.all();
		
		double amount = 0;
		for (Row result: results) {
			amount += result.getDecimal("ol_amount").doubleValue();
		}
		return new BigDecimal(amount);
	}
	
	private Row getCustomer(int w_id, int d_id, int c_id) {
		Session session = getSession();
		PreparedStatement ps = session.prepare(
			"SELECT c_balance, c_delivery_cnt " +
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
	
	private ResultSet updateOrder(int w_id, int d_id, int o_id, int o_carrier_id) {
		Session session = getSession();
		PreparedStatement ps = session.prepare(
			"UPDATE orders " +
			"SET o_carrier_id = ? " +
			"WHERE o_w_id = ? AND o_d_id = ? AND o_id = ? " +
			"IF o_carrier_id = -1;"
		).setConsistencyLevel(writeConsistencyLevel);
		return session.execute(ps.bind(o_carrier_id, w_id, d_id, o_id));
	}
	
	private ResultSet updateOrderLines(int w_id, int d_id, int o_id, int o_ol_cnt) {
		Session session = getSession();
		Date ol_delivery_d = new Date();
		String olNumbersString = Arrays.toString(IntStream.range(1, o_ol_cnt + 1).toArray());
		olNumbersString = olNumbersString.substring(1, olNumbersString.length() - 1);
		PreparedStatement ps = session.prepare(
			"UPDATE orderlines " +
			"SET ol_delivery_d = ? " +
			"WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ? " +
			"AND ol_number IN (" + olNumbersString + ");"
		).setConsistencyLevel(writeConsistencyLevel);
		return session.execute(ps.bind(ol_delivery_d, w_id, d_id, o_id));
	}
	
	private ResultSet updateCustomer(
			int w_id, int d_id, int c_id, BigDecimal c_balance, int c_delivery_cnt) {
		Session session = getSession();
		PreparedStatement ps = session.prepare(
			"UPDATE customers " +
			"SET c_balance = ?, " +
			    "c_delivery_cnt = ? " +
			"WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?;"
		).setConsistencyLevel(writeConsistencyLevel);
		return session.execute(ps.bind(c_balance, c_delivery_cnt, w_id, d_id, c_id));
	}
}
