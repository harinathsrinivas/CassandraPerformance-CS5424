import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.List;
import java.util.Set;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class RelatedCustomer extends Transaction {
	private static int THRESHOLD = 2;
	
	public RelatedCustomer(Session session, ConsistencyLevel writeConsistencyLevel) {
		super(session, writeConsistencyLevel);
	}

	@Override
	public void process(String[] args) {
		System.out.println("-------- Related-Customer --------");
		int w_id = Integer.parseInt(args[1]);
		int d_id = Integer.parseInt(args[2]);
		int c_id = Integer.parseInt(args[3]);
		
		List<Integer> otherWIds = new ArrayList<Integer>();
		for (int i=1; i <= 10; ++i) {
			if (i != w_id) otherWIds.add(i);
		}
		
		Set<List<Integer>> relatedCustomers = new HashSet<List<Integer>>();
		List<Row> orders = getOrders(w_id, d_id, c_id);
		for (Row order: orders) {
			int o_id = order.getInt("o_id");
			List<Row> orderLines = getOrderLines(w_id, d_id, o_id);
			List<Integer> items = new ArrayList<Integer>();
			for (Row orderLine: orderLines) {
				items.add(orderLine.getInt("ol_i_id"));
			}
			addRelatedCustomers(otherWIds, items, relatedCustomers);
		}
		
		for (List<Integer> customer: relatedCustomers) {
			System.out.printf(
					"(C_W_ID, C_D_ID, C_ID): (%d, %d, %d)\n",
					customer.get(0), customer.get(1), customer.get(2)
			);
		}
	}

	private Set<List<Integer>> addRelatedCustomers(
			List<Integer> otherWIds, List<Integer> items, Set<List<Integer>> relatedCustomers) {
		Session session = getSession();
		String itemsString = items.toString();
		itemsString = itemsString.substring(1, itemsString.length() - 1);
		String wIdsString = otherWIds.toString();
		wIdsString = wIdsString.substring(1, wIdsString.length() - 1);
		
		String query = 
			"SELECT relatedorder(ol_w_id, ol_d_id, ol_o_id) as related " +
		    "FROM orderlinesbyitem " +
		    "WHERE ol_i_id IN (" + itemsString + ")" +
		    "AND ol_w_id IN (" + wIdsString + ");";
		ResultSet resultSet = session.execute(query);
		List<Row> results = resultSet.all();
		
		if (results.isEmpty()) {
			return relatedCustomers;
		}
		
	    Map<String,Integer> relatedOrders = results.get(0).getMap("related", String.class, Integer.class);
	    for (String orderString: relatedOrders.keySet()) {
	    	if (relatedOrders.get(orderString) >= THRESHOLD) {
	    		String[] ids = orderString.split(",");
	    		int w_id = Integer.parseInt(ids[0]);
	    		int d_id = Integer.parseInt(ids[1]);
	    		int o_id = Integer.parseInt(ids[2]);
	    		int c_id = getCustomerId(w_id, d_id, o_id);
	    		List<Integer> customer = Arrays.asList(w_id, d_id, c_id);
	    		relatedCustomers.add(customer);
	    	}
	    }
		return relatedCustomers;
	}

	private List<Row> getOrders(int w_id, int d_id, int c_id) {
		Session session = getSession();
		PreparedStatement ps = session.prepare(
			"SELECT o_id " +
		    "FROM orders " +
		    "WHERE o_w_id = ? AND o_d_id = ? AND o_c_id = ? " +
		    "ALLOW FILTERING;"
		);
		ResultSet resultSet = session.execute(ps.bind(w_id, d_id, c_id));
		List<Row> results = resultSet.all();
		return results;
	}
	
	private List<Row> getOrderLines(int w_id, int d_id, int o_id) {
		Session session = getSession();
		PreparedStatement ps = session.prepare(
			"SELECT ol_i_id " +
		    "FROM orderlines " +
		    "WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?" +
		    "ALLOW FILTERING;"
		);
		ResultSet resultSet = session.execute(ps.bind(w_id, d_id, o_id));
		List<Row> results = resultSet.all();
		return results;
	}
	
	private int getCustomerId(int w_id, int d_id, int o_id) {
		Session session = getSession();
		PreparedStatement ps = session.prepare(
			"SELECT o_c_id " +
		    "FROM orders " +
		    "WHERE o_w_id = ? AND o_d_id = ? AND o_id = ?;"
		);
		ResultSet resultSet = session.execute(ps.bind(w_id, d_id, o_id));
		List<Row> results = resultSet.all();
		
		if (results.isEmpty()) {
			throw new IllegalArgumentException("No matching order");
		}
		return results.get(0).getInt("o_c_id");
	}
}
