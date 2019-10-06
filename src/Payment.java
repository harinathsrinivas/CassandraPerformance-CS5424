import java.math.BigDecimal;
import java.util.List;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class Payment extends Transaction {

	public Payment(Session session, ConsistencyLevel writeConsistencyLevel) {
		super(session, writeConsistencyLevel);
	}

	@Override
	public void process(String[] args) {
		System.out.println("-------- Payment --------");
		int w_id = Integer.parseInt(args[1]);
		int d_id = Integer.parseInt(args[2]);
		int c_id = Integer.parseInt(args[3]);
		BigDecimal payment = new BigDecimal(args[4]);

		Row warehouse = getWarehouse(w_id);
		BigDecimal w_ytd = warehouse.getDecimal("w_ytd").add(payment);
		updateWarehouse(w_id, w_ytd);
		
		Row district = getDistrict(w_id, d_id);
		BigDecimal d_ytd = district.getDecimal("d_ytd").add(payment);
		updateDistrict(w_id, d_id, d_ytd);		
		
		Row customer = getCustomer(w_id, d_id, c_id);
		BigDecimal c_balance = customer.getDecimal("c_balance").add(payment.negate());
		float c_ytd_payment = customer.getFloat("c_ytd_payment") + payment.floatValue();
		int c_payment_cnt = customer.getInt("c_payment_cnt") + 1;
		updateCustomer(w_id, d_id, c_id, c_balance, c_ytd_payment, c_payment_cnt);
		

		
		//output
		System.out.printf(
				"C_IDENTIFIER: (%d, %d, %d), C_NAME: (%s, %s, %s), " +
				"C_ADDRESS: (%s, %s, %s ,%s, %s), C_PHONE: %s, C_SINCE: %tc, " +
				"C_CREDIT: %s,  C_CREDIT_LIM: %f, C_DISCOUNT: %f, C_BALANCE: %f\n",
				w_id, d_id, c_id, 
				customer.getString("c_first"), 
				customer.getString("c_middle"), 
				customer.getString("c_last"), 
				customer.getString("c_street_1"), 
				customer.getString("c_street_2"),
				customer.getString("c_city"), 
				customer.getString("c_state"), 
				customer.getString("c_zip"), 
				customer.getString("c_phone"),
				customer.getTimestamp("c_since"),
				customer.getString("c_credit"),
				customer.getDecimal("c_credit_lim"),
				customer.getDecimal("c_discount"),
				c_balance
		);
		System.out.printf(
				"W_ADDRESS: (%s, %s, %s, %s, %s) \n",
				warehouse.getString("w_street_1"), 
				warehouse.getString("w_street_2"),
				warehouse.getString("w_city"), 
				warehouse.getString("w_state"), 
				warehouse.getString("w_zip")
		);
		System.out.printf(
				"D_ADDRESS: (%s, %s, %s, %s, %s) \n",
				district.getString("d_street_1"), 
				district.getString("d_street_2"),
				district.getString("d_city"), 
				district.getString("d_state"), 
				district.getString("d_zip")
		);
	}
	
	private Row getWarehouse(int w_id) {
		Session session = getSession();
		PreparedStatement ps = session.prepare(
			"SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_ytd " + 
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
			"SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_ytd " + 
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

	private Row getCustomer(int w_id, int d_id, int c_id) {
		Session session = getSession();
		PreparedStatement ps = session.prepare(
			"SELECT c_first, c_middle, c_last, c_street_1, c_street_2, " + 
				   "c_city, c_state, c_zip, c_phone, c_since, c_credit, " +
				   "c_credit_lim, c_discount, c_balance, c_ytd_payment, c_payment_cnt " +
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
	
	private ResultSet updateWarehouse(int w_id, BigDecimal w_ytd) {
		Session session = getSession();
		PreparedStatement ps = session.prepare(
			"UPDATE warehouses " +
			"SET w_ytd = ?" +
			"WHERE w_id = ?;"
		).setConsistencyLevel(writeConsistencyLevel);
		return session.execute(ps.bind(w_ytd, w_id));
	}
	
	private ResultSet updateDistrict(int w_id, int d_id, BigDecimal d_ytd) {
		Session session = getSession();
		PreparedStatement ps = session.prepare(
			"UPDATE districts " +
			"SET d_ytd = ? " +
			"WHERE d_w_id = ? AND d_id = ?;"
		).setConsistencyLevel(writeConsistencyLevel);
		return session.execute(ps.bind(d_ytd, w_id, d_id));
	}
	
	private ResultSet updateCustomer(
			int w_id, int d_id, int c_id, BigDecimal c_balance,
			float c_ytd_payment, int c_payment_cnt) {
		Session session = getSession();
		PreparedStatement ps = session.prepare(
			"UPDATE customers " +
			"SET c_balance = ?, " +
			    "c_ytd_payment = ?, " +
			    "c_payment_cnt = ? " +
			"WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?;"
		).setConsistencyLevel(writeConsistencyLevel);
		return session.execute(
				ps.bind(c_balance, c_ytd_payment, c_payment_cnt, w_id, d_id, c_id));
	}
}
