import java.util.List;
import java.util.ArrayList;
import java.util.Date;
import java.math.BigDecimal;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;


public class NewOrder extends Transaction {
	private class OrderLine {
		private int n;
		private int i_id;
		private int w_id;
		private BigDecimal quantity;
		
		public OrderLine(int n, int i_id, int w_id, BigDecimal quantity) {
			this.n = n;
			this.i_id = i_id;
			this.w_id = w_id;
			this.quantity = quantity;
		}

		public int getN() {
			return n;
		}

		public int getIId() {
			return i_id;
		}

		public int getWId() {
			return w_id;
		}

		public BigDecimal getQuantity() {
			return quantity;
		}
	}
	
	public NewOrder(Session session, ConsistencyLevel writeConsistencyLevel) {
		super(session, writeConsistencyLevel);
	}

	@Override
	public void process(String[] args) {
		System.out.println("-------- New Order --------");
		int c_id = Integer.parseInt(args[1]);
		int w_id = Integer.parseInt(args[2]);
		int d_id = Integer.parseInt(args[3]);
		BigDecimal o_ol_cnt = new BigDecimal(args[4]);
		List<OrderLine> orderLines = parseOrderLines(args[5]);
		Row customer = getCustomer(w_id, d_id, c_id);
		Row warehouse = getWarehouse(w_id);
		
		Row district = getDistrict(w_id, d_id);
		int o_id = district.getInt("d_next_o_id");
		BigDecimal o_all_local = new BigDecimal(isAllLocal(w_id, orderLines) ? 1 : 0);
		ResultSet updateOrder;
		do {
			updateOrder = insertOrder(w_id, d_id, o_id, c_id, o_ol_cnt, o_all_local);
			updateOrder.all();
			o_id++;
		} while (!updateOrder.wasApplied());
		o_id = o_id - 1;
		int d_next_o_id = o_id + 1;
		updateDNextOId(w_id, d_id, d_next_o_id);
		
		//output
		String c_last = customer.getString("c_last");
		String c_credit = customer.getString("c_credit");
		BigDecimal c_discount = customer.getDecimal("c_discount");
		System.out.printf(
				"(W_ID, D_ID, C_ID): (%d, %d, %d), C_LAST: %s, C_CREDIT: %s, C_DISCOUNT: %f\n",
				w_id, d_id, c_id, c_last, c_credit, c_discount.floatValue()
		);
		BigDecimal w_tax = warehouse.getDecimal("w_tax");
		BigDecimal d_tax = district.getDecimal("d_tax");
		System.out.printf("W_TAX: %f, D_TAX: %f\n", 
				w_tax.floatValue(), d_tax.floatValue());
		
		
		double totalAmount = insertOrderLines(w_id, d_id, o_id, orderLines);

		BigDecimal c_tax = customer.getDecimal("c_discount");
		totalAmount = totalAmount 
				* (1 + w_tax.add(d_tax).doubleValue()) 
				* (1 - c_tax.doubleValue());

		//output
		System.out.printf("NUM_ITEMS: %d, TOTAL_AMOUNT: %f\n", 
				o_ol_cnt.toBigInteger(), totalAmount);
	}
	
	private List<OrderLine> parseOrderLines(String orderLinesString) {
		List<OrderLine> orderLines = new ArrayList<OrderLine>();
		String[] orderLinesStrArr = orderLinesString.split("\n");
		for (int i = 0; i < orderLinesStrArr.length; i++) {
			String[] args = orderLinesStrArr[i].split(",");
			OrderLine orderLine = new OrderLine(
					i + 1,
					Integer.parseInt(args[0]),
					Integer.parseInt(args[1]),
					new BigDecimal(args[2]));
			orderLines.add(orderLine);
		}
		return orderLines;
	}

	private Row getCustomer(int w_id, int d_id, int c_id) {
		Session session = getSession();
		PreparedStatement ps = session.prepare(
			"SELECT c_last, c_credit, c_discount " + 
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
	
	private Row getWarehouse(int w_id) {
		Session session = getSession();
		PreparedStatement ps = session.prepare(
			"SELECT w_tax " + 
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
			"SELECT d_tax, d_next_o_id " + 
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
	
	private Row getItem(int i_id) {
		Session session = getSession();
		PreparedStatement ps = session.prepare(
			"SELECT i_name, i_price " + 
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
	
	private double insertOrderLines(int w_id, int d_id, int o_id, List<OrderLine> orderLines) {
		double totalAmount = 0;
		String ol_dist_info = String.format("S_DIST_%02d", d_id);
		for (OrderLine orderLine : orderLines) {
			int ol_supply_w_id = orderLine.getWId();
			int i_id = orderLine.getIId();
			BigDecimal ol_quantity = orderLine.getQuantity();
			BigDecimal s_quantity;
			ResultSet stockUpdate;
			do {
				Row stock = getStock(ol_supply_w_id, i_id, ol_dist_info);
				s_quantity = stock.getDecimal("s_quantity");
				BigDecimal s_ytd = stock.getDecimal("s_quantity");
				int s_order_cnt = stock.getInt("s_order_cnt");
				int s_remote_cnt = stock.getInt("s_remote_cnt");
				
				s_quantity = s_quantity.add(ol_quantity.negate());
				if (s_quantity.doubleValue() < 10) {
					s_quantity = s_quantity.add(new BigDecimal(100));
				}
				s_ytd = s_ytd.add(ol_quantity);
				s_order_cnt++;
				if (w_id != ol_supply_w_id) { s_remote_cnt++; }; 
				stockUpdate = updateStock(ol_supply_w_id, i_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt);
			} while (!stockUpdate.wasApplied());
			Row item = getItem(i_id);
			BigDecimal i_price = item.getDecimal("i_price");
			String i_name = item.getString("i_name");
			
			int ol_n = orderLine.getN();
			Date ol_delivery_d = null;
			BigDecimal ol_amount = ol_quantity.multiply(i_price);
			insertOrderLine(
					w_id, d_id, o_id, ol_n, i_id, ol_delivery_d, ol_amount,
					ol_supply_w_id, ol_quantity, ol_dist_info);
			totalAmount += ol_amount.doubleValue();
			
			System.out.printf(
					"  ITEM_NUMBER: %d, I_NAME: %s, SUPPLIER_WAREHOUSE: %d, QUANTITY: %d, OL_AMOUNT: %f, S_QUANTITY: %d\n",
					i_id, i_name, ol_supply_w_id, ol_quantity.toBigInteger(), ol_amount, s_quantity.toBigInteger()
			);
		}
		return totalAmount;
	}
	
	private ResultSet insertOrderLine(
			int w_id, int d_id, int o_id, int ol_n, int i_id, 
			Date ol_delivery_d, BigDecimal ol_amount,
			int ol_supply_w_id, BigDecimal ol_quantity, String ol_dist_info) {
		
		Session session = getSession();
		PreparedStatement ps = session.prepare(
	        "INSERT INTO orderlines (" +
	            "ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, " +
	            "ol_delivery_d, ol_amount, ol_supply_w_id, ol_quantity, ol_dist_info) " +
	        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"
		).setConsistencyLevel(writeConsistencyLevel);
		
		ResultSet resultSet = session.execute(ps.bind(
				w_id, d_id, o_id, ol_n, i_id, ol_delivery_d, ol_amount,
				ol_supply_w_id, ol_quantity, ol_dist_info));
		return resultSet;
	}
	
	private Row getStock(int w_id, int i_id, String s_dist_xx) {
		Session session = getSession();
		PreparedStatement ps = session.prepare(
			"SELECT s_quantity, s_ytd, s_order_cnt, s_remote_cnt, " +
			        s_dist_xx + " " +
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
	
	private ResultSet updateStock(
			int w_id, int i_id, BigDecimal s_quantity, BigDecimal s_ytd,
			int s_order_cnt, int s_remote_cnt) {
		Session session = getSession();
		PreparedStatement ps = session.prepare(
				"UPDATE stock " +
				"SET s_quantity = ?, s_ytd = ?, s_order_cnt = ?, s_remote_cnt = ? " +
				"WHERE s_w_id = ? AND s_i_id = ? IF s_order_cnt = ?"
		).setConsistencyLevel(writeConsistencyLevel);
		int old_s_order_cnt = s_order_cnt - 1;
		ResultSet resultSet = session.execute(ps.bind(
				s_quantity, s_ytd, s_order_cnt, s_remote_cnt, w_id, i_id, old_s_order_cnt));
		return resultSet;
	}
	
	private boolean isAllLocal(int w_id, List<OrderLine> orderLines) {
		for (OrderLine orderLine : orderLines) {
			int s_w_id = orderLine.getWId();
			if (w_id == s_w_id) { 
				return false; 
			}
		}
		return true;
	}
	
	private ResultSet insertOrder(int w_id, int d_id, int o_id, int c_id, BigDecimal o_ol_cnt, BigDecimal o_all_local) {
		Session session = getSession();
		Date o_entry_d = new Date();
		int o_carrier_id = -1;
		PreparedStatement ps = session.prepare(
			"INSERT INTO orders (o_w_id, o_d_id, o_id, o_c_id, o_carrier_id, " +
				                "o_ol_cnt, o_all_local, o_entry_d) " +
			"VALUES (?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS;"
		).setConsistencyLevel(writeConsistencyLevel);
		ResultSet resultSet = session.execute(
				ps.bind(w_id, d_id, o_id, c_id, o_carrier_id, o_ol_cnt, o_all_local, o_entry_d));	
		return resultSet;
	}
		
	private ResultSet updateDNextOId(int w_id, int d_id, int d_next_o_id) {
		Session session = getSession();
		PreparedStatement ps = session.prepare(
			"UPDATE districts " +
			"SET d_next_o_id = ? " +
			"WHERE d_w_id = ? AND d_id = ? IF d_next_o_id < ?;"
		).setConsistencyLevel(writeConsistencyLevel);
		int old_d_next_o_id = d_next_o_id - 1;
		ResultSet resultSet = session.execute(ps.bind(d_next_o_id, w_id, d_id, old_d_next_o_id));	
		return resultSet;
	}
}
