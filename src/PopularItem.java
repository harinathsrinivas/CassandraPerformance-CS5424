import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class PopularItem extends Transaction {

	public PopularItem(Session session, ConsistencyLevel writeConsistencyLevel) {
		super(session, writeConsistencyLevel);
	}

	@Override
	public void process(String[] args) {
		System.out.println("-------- Popular-Item --------");
		int w_id = Integer.parseInt(args[1]);
		int d_id = Integer.parseInt(args[2]);
		int l = Integer.parseInt(args[3]);
		
		Row district = getDistrict(w_id, d_id);
		int d_next_o_id = district.getInt("d_next_o_id");
		int gt_o_id = d_next_o_id - l - 1;
		List<Row> orders = getOrders(w_id, d_id, gt_o_id);
		
		System.out.printf(
				"(W_ID, D_ID): (%d, %d)\n" +
				"L: %d\n",
				w_id, d_id, l
		);
		
		Map<String, Integer> items = new HashMap<String, Integer>();
		for (Row order: orders) {
			int o_id = order.getInt("o_id");
			int c_id = order.getInt("o_c_id");
			Row customer = getCustomer(w_id, d_id, c_id);
			
			System.out.printf(
					"O_ID: %d, O_ENTRY_D: %tc, C_NAME: (%s, %s, %s)\n",
					o_id, 
					order.getTimestamp("o_entry_d"), 
					customer.getString("c_first"), 
					customer.getString("c_middle"), 
					customer.getString("c_last")
			);
			
			List<Row> orderLines = getMaxOrderLines(w_id, d_id, o_id);
			for (Row orderLine: orderLines) {
					int i_id = orderLine.getInt("ol_i_id");
					Row item = getItem(i_id);
					String i_name = item.getString("i_name");
					int count = items.containsKey(i_name) ? items.get(i_name) : 0;
					items.put(i_name, count + 1);
					
					System.out.printf(
							"I_NAME: %s, OL_QUANTITY: %d\n",
							i_name, orderLine.getDecimal("ol_quantity").intValue()
					);
			}
		}
		
		for (String i_name: items.keySet()) {
			int count = items.get(i_name);
			double percent = (double) count / (double) l * 100.0;
			
			System.out.printf(
					"I_NAME: %s, PERCENT OF ORDERS: %f percent\n",
					i_name, percent
			);
		}
	}

	private Row getDistrict(int w_id, int d_id) {
		Session session = getSession();
		PreparedStatement ps = session.prepare(
			"SELECT d_next_o_id " + 
		    "FROM districts " + 
			"WHERE d_w_id = ? AND d_id = ?;"
		);
		ResultSet resultSet = session.execute(ps.bind(w_id, d_id));
		List<Row> results = resultSet.all();
		
		if (results.isEmpty()) {
			throw new IllegalArgumentException("No matching district");
		}
		return results.get(0);
	}

	private List<Row> getOrders(int w_id, int d_id, int gt_o_id) {
		Session session = getSession();
		PreparedStatement ps = session.prepare(
			"SELECT o_id, o_c_id, o_entry_d " +
		    "FROM orders " +
		    "WHERE o_w_id = ? AND o_d_id = ? AND o_id > ?;"
		);
		ResultSet resultSet = session.execute(ps.bind(w_id, d_id, gt_o_id));
		List<Row> results = resultSet.all();
		return results;
	}

	private List<Row> getMaxOrderLines(int w_id, int d_id, int o_id) {
		Session session = getSession();
		BigDecimal ol_quantity = getMaxQuantity(w_id, d_id, o_id);
		PreparedStatement ps = session.prepare(
			"SELECT ol_i_id, ol_quantity " +
		    "FROM orderlines " +
		    "WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ? AND ol_quantity = ?" +
		    "ALLOW FILTERING;"
		);
		ResultSet resultSet = session.execute(ps.bind(w_id, d_id, o_id, ol_quantity));
		List<Row> results = resultSet.all();
		return results;
	}
	
	private BigDecimal getMaxQuantity(int w_id, int d_id, int o_id) {
		Session session = getSession();
		PreparedStatement ps = session.prepare(
			"SELECT max(ol_quantity) as max_ol_quantity " +
		    "FROM orderlines " +
		    "WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?;"
		);
		ResultSet resultSet = session.execute(ps.bind(w_id, d_id, o_id));
		List<Row> results = resultSet.all();
		
		if (results.get(0) == null) {
			throw new IllegalArgumentException("No matching orderline");
		}
		return results.get(0).getDecimal("max_ol_quantity");
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
	
	private Row getItem(int i_id) {
		Session session = getSession();
		PreparedStatement ps = session.prepare(
			"SELECT i_name " + 
		    "FROM items " + 
			"WHERE i_id = ?;"
		);
		ResultSet resultSet = session.execute(ps.bind(i_id));
		List<Row> results = resultSet.all();
		
		if (results.isEmpty()) {
			throw new IllegalArgumentException("No matching item");
		}
		return results.get(0);
	}
}
