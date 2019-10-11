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
			ResultSet updateOrder;
			int o_id;
			int o_ol_cnt;
			do {
				order = getOrder(w_id, d_id);
				if (order == null) break;
				o_id = order.getInt("o_id");
				updateOrder = updateOrder(w_id, d_id, o_id, o_carrier_id);
			} while (!updateOrder.wasApplied());
			if (order == null) continue;  // all orders are delivered
			o_ol_cnt = order.getDecimal("o_ol_cnt").intValue();
			o_id = order.getInt("o_id");
			updateOrderLines(w_id, d_id, o_id, o_ol_cnt);
			int c_id = order.getInt("o_c_id");
			BigDecimal totalAmount = getTotalAmount(w_id, d_id, o_id);
			ResultSet customerUpdate;
			do {
				Row customer = getCustomer(w_id, d_id, c_id);
				BigDecimal c_balance = customer.getDecimal("c_balance").add(totalAmount);
				int c_delivery_cnt = customer.getInt("c_delivery_cnt") + 1;
				customerUpdate = updateCustomer(w_id, d_id, c_id, c_balance, c_delivery_cnt);
			} while (!customerUpdate.wasApplied());
			System.out.printf("(W_ID, D_ID, C_ID, O_ID): (%d, %d, %d, %d)\n", 
							  w_id, d_id, o_id, c_id);
		}
	}

	private Row getOrder(int w_id, int d_id) {
		Session session = getSession();
		PreparedStatement ps = session.prepare(
			"SELECT o_id, o_c_id, o_ol_cnt " +
		    "FROM ordersbyid " +
		    "WHERE o_w_id = ? AND o_d_id = ? AND o_carrier_id = -1 " +
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
			"SELECT sum(ol_amount) as sum_ol_amount " +
		    "FROM orderlines " +
		    "WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?;"
		);
		ResultSet resultSet = session.execute(ps.bind(w_id, d_id, o_id));
		List<Row> results = resultSet.all();
		
		if (results.isEmpty()) {
			throw new IllegalArgumentException("No matching orderlines");
		}
		return results.get(0).getDecimal("sum_ol_amount");
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
		Session session = getSession();  //
		PreparedStatement ps = session.prepare(
			"UPDATE orders " +
			"SET o_carrier_id = ? " +
			"WHERE o_w_id = ? AND o_d_id = ? AND o_id = ? " +
			"IF o_carrier_id = -1;"
		).setConsistencyLevel(writeConsistencyLevel);
		ResultSet resultSet = session.execute(ps.bind(o_carrier_id, w_id, d_id, o_id));
		return resultSet;
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
		ResultSet resultSet = session.execute(ps.bind(ol_delivery_d, w_id, d_id, o_id));
		return resultSet;
	}
	
	private ResultSet updateCustomer(
			int w_id, int d_id, int c_id, BigDecimal c_balance, int c_delivery_cnt) {
		Session session = getSession();
		PreparedStatement ps = session.prepare(
			"UPDATE customers " +
			"SET c_balance = ?, " +
			    "c_delivery_cnt = ? " +
			"WHERE c_w_id = ? AND c_d_id = ? AND c_id = ? IF c_delivery_cnt = ?;"
		).setConsistencyLevel(writeConsistencyLevel);
		int old_c_delivery_cnt = c_delivery_cnt - 1;
		ResultSet resultSet = session.execute(
				ps.bind(c_balance, c_delivery_cnt, w_id, d_id, c_id, old_c_delivery_cnt));
		return resultSet;
	}
}
