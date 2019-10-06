import java.util.List;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class TopBalance extends Transaction {

	public TopBalance(Session session, ConsistencyLevel writeConsistencyLevel) {
		super(session, writeConsistencyLevel);
	}

	@Override
	public void process(String[] args) {
		System.out.println("-------- Top-Balance --------");
		int TOP_N = 10;
		List<Row> customers = getTopCustomers(TOP_N);
		
		for (Row customer: customers) {
			int w_id = customer.getInt("c_w_id");
			int d_id = customer.getInt("c_d_id");
			Row warehouse = getWarehouse(w_id);
			Row district = getDistrict(w_id, d_id);
			
			System.out.printf(
					"C_NAME: (%s, %s, %s), C_BALANCE: %f, W_NAME: %s, D_NAME: %s\n",
					customer.getString("c_first"), 
					customer.getString("c_middle"), 
					customer.getString("c_last"), 
					customer.getDecimal("c_balance"),
					warehouse.getString("w_name"),
					district.getString("d_name")
			);
		}
	}
	
	private List<Row> getTopCustomers(int top_n) {
		Session session = getSession();
		PreparedStatement ps = session.prepare(
			"SELECT c_w_id, c_d_id, c_balance, c_first, c_middle, c_last " + 
		    "FROM customersbybalance " + 
			"LIMIT ? ALLOW FILTERING;"
		);
		ResultSet resultSet = session.execute(ps.bind(top_n));
		List<Row> results = resultSet.all();
		return results;
	}
	
	private Row getWarehouse(int w_id) {
		Session session = getSession();
		PreparedStatement ps = session.prepare(
			"SELECT w_name " + 
		    "FROM warehouses " + 
			"WHERE w_id = ?;"
		);
		ResultSet resultSet = session.execute(ps.bind(w_id));
		List<Row> results = resultSet.all();
		
		if (results.isEmpty()) {
			throw new IllegalArgumentException("No matching warehouse");
		}
		return results.get(0);
	}
	
	private Row getDistrict(int w_id, int d_id) {
		Session session = getSession();
		PreparedStatement ps = session.prepare(
			"SELECT d_name " + 
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
}