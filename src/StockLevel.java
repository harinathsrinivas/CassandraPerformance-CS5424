import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class StockLevel extends Transaction {

	public StockLevel(Session session, ConsistencyLevel writeConsistencyLevel) {
		super(session, writeConsistencyLevel);
	}

	@Override
	public void process(String[] args) {
		System.out.println("-------- Stock-Level --------");
		int w_id = Integer.parseInt(args[1]);
		int d_id = Integer.parseInt(args[2]);
		int t = Integer.parseInt(args[3]);
		int l = Integer.parseInt(args[4]);
		
		Row district = getDistrict(w_id, d_id);
		int d_next_o_id = district.getInt("d_next_o_id");
		int gt_o_id = d_next_o_id - l - 1;
		List<Row> orderLines = getOrderLines(w_id, d_id, gt_o_id);
		
		Set<Integer> items = new HashSet<Integer>();
		for (Row orderLine: orderLines) {
				int i_id = orderLine.getInt("ol_i_id");
				items.add(i_id);
		}
		
		int count = 0;
		for (int i_id: items) {
			Row stock = getStock(w_id, i_id);
			if (stock.getDecimal("s_quantity").intValue() < t) {
				count++;
			}
		}
		
		System.out.printf(
				"W_ID: %d\n" +
				"NUMBER OF ITEMS S_QUANTITY < %d: %d\n",
				w_id, t, count
		);
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
	
	private List<Row> getOrderLines(int w_id, int d_id, int gt_o_id) {
		Session session = getSession();
		PreparedStatement ps = session.prepare(
			"SELECT ol_i_id " +
		    "FROM orderlines " +
		    "WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id > ? " +
		    "ALLOW FILTERING;"
		);
		ResultSet resultSet = session.execute(ps.bind(w_id, d_id, gt_o_id));
		List<Row> results = resultSet.all();
		return results;
	}
	
	private Row getStock(int w_id, int i_id) {
		Session session = getSession();
		PreparedStatement ps = session.prepare(
			"SELECT s_quantity " +
		    "FROM stock " +
		    "WHERE s_w_id = ? AND s_i_id = ?;"
		);
		
		ResultSet resultSet = session.execute(ps.bind(w_id, i_id));
		List<Row> results = resultSet.all();
		
		if (results.isEmpty()) {
			throw new IllegalArgumentException("No matching stock");
		}
		return results.get(0);
	}
}
