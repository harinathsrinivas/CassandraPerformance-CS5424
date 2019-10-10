import java.util.Arrays;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.schemabuilder.Create;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.datastax.driver.core.DataType;

public class LoadData {
	//private static String cqlshPath = "/usr/local/bin/cqlsh";
	private static String cqlshPath = "/temp/Cassandra/bin/cqlsh";
	//private static String dataPath = "project-files/data-files/";
	private static String dataPath = "/temp/project-files/data-files/";
	//private static String serverIP = "127.0.0.1";
	private static String serverIP = "192.168.56.159";
	private static String keyspace = "cs5424";
	private static Session session;
	private static Cluster cluster;
	
	
	public static void main(String[] args) {
		if(args.length == 1){
			serverIP = args[0];
			System.out.println("Running the program in IP: "+serverIP);
		}
		else if(args.length > 1){
			System.out.println("Wrong number of command line arguments - expected 1 argument - Ip address...");
			System.exit(2);
		}

		String replicationStrategy = "SimpleStrategy";
		int replicationFactor = 3;
		
		startSession();
		// keyspace
		deleteKeyspace(keyspace);
		createKeyspace(keyspace, replicationStrategy, replicationFactor);
		useKeyspace(keyspace);
		// tables
		createWarehouseTable();
		loadWarehouseData(dataPath + "warehouse.csv");
		createDistrictTable();
		loadDistrictData(dataPath + "district.csv");
		createCustomerTable();
		createCustomerByBalanceView();
		loadCustomerData(dataPath + "customer.csv");
		createOrderTable();
		loadOrderData(dataPath + "order.csv");
		createItemTable();
		loadItemData(dataPath + "item.csv");
		createOrderLineTable();
		createOrderLineByItemView();
		createRelatedOrderFunc();
		loadOrderLineData(dataPath + "order-line.csv");
		createStockTable();
		loadStockData(dataPath + "stock.csv");
		closeSession();
	}

	public static void startSession() {
		cluster = Cluster.builder()
				  .addContactPoints(serverIP)
				  .build();
		session = cluster.connect();
	}
	
    public static void closeSession() {
        session.close();
        cluster.close();
    }
	
	public static void createKeyspace(
		String keyspace, 
		String replicationStrategy, 
		int replicationFactor) {
		String query = 
				"CREATE KEYSPACE IF NOT EXISTS " + keyspace + " " +
				"WITH replication = {'class': '" + replicationStrategy + "'," +
						"'replication_factor':" + replicationFactor + "};";
	    session.execute(query);
	}
	
	public static void useKeyspace(String keyspace) {
		session.close();
		session = cluster.connect(keyspace);
	}
	
	public static void deleteKeyspace(String keyspaceName) {
		String query = "DROP KEYSPACE IF EXISTS " + keyspaceName;
		session.execute(query);
	}
	
	public static void createWarehouseTable() {
		Create create = SchemaBuilder.createTable("warehouses")
				                     .addPartitionKey("W_ID", DataType.cint())
									 .addColumn("W_NAME", DataType.varchar())
							  	  	 .addColumn("W_STREET_1", DataType.varchar())
							  	  	 .addColumn("W_STREET_2", DataType.varchar())
							  	  	 .addColumn("W_CITY", DataType.varchar())
							  	  	 .addColumn("W_STATE", DataType.varchar())
							  	  	 .addColumn("W_ZIP", DataType.varchar())
							  	  	 .addColumn("W_TAX", DataType.decimal())
							  	  	 .addColumn("W_YTD", DataType.decimal());
		session.execute(create);
	}
	
	public static void loadWarehouseData(String filepath) {
		String[] columnNames = {"W_ID", "W_NAME", "W_STREET_1", "W_STREET_2", "W_CITY", 
				                "W_STATE", "W_ZIP", "W_TAX", "W_YTD"};
			loadFromCsv("warehouses", columnNames, filepath);
	}
	
	public static void createDistrictTable() {
		Create create = SchemaBuilder.createTable("districts")
				                     .addPartitionKey("D_W_ID", DataType.cint())
				                     .addPartitionKey("D_ID", DataType.cint())
									 .addColumn("D_NAME", DataType.varchar())
							  	  	 .addColumn("D_STREET_1", DataType.varchar())
							  	  	 .addColumn("D_STREET_2", DataType.varchar())
							  	  	 .addColumn("D_CITY", DataType.varchar())
							  	  	 .addColumn("D_STATE", DataType.varchar())
							  	  	 .addColumn("D_ZIP", DataType.varchar())
							  	  	 .addColumn("D_TAX", DataType.decimal())
							  	  	 .addColumn("D_YTD", DataType.decimal())
								 	 .addColumn("D_NEXT_O_ID", DataType.cint());
		session.execute(create);
	}
	
	public static void loadDistrictData(String filepath) {
		String[] columnNames = {"D_W_ID", "D_ID", "D_NAME", "D_STREET_1", "D_STREET_2", 
								"D_CITY", "D_STATE", "D_ZIP", "D_TAX", "D_YTD", "D_NEXT_O_ID"};
		loadFromCsv("districts", columnNames, filepath);
	}
	
	public static void createCustomerTable() {
		Create create = SchemaBuilder.createTable("customers")
				                     .addPartitionKey("C_W_ID", DataType.cint())
				                     .addPartitionKey("C_D_ID", DataType.cint())
				                     .addPartitionKey("C_ID", DataType.cint())
				                     .addColumn("C_FIRST", DataType.varchar())
				                     .addColumn("C_MIDDLE", DataType.varchar())
									 .addColumn("C_LAST", DataType.varchar())
							  	  	 .addColumn("C_STREET_1", DataType.varchar())
							  	  	 .addColumn("C_STREET_2", DataType.varchar())
							  	  	 .addColumn("C_CITY", DataType.varchar())
							  	  	 .addColumn("C_STATE", DataType.varchar())
							  	  	 .addColumn("C_ZIP", DataType.varchar())
							  	  	 .addColumn("C_PHONE", DataType.varchar())
							  	  	 .addColumn("C_SINCE", DataType.timestamp())
							  	  	 .addColumn("C_CREDIT", DataType.varchar())
							  	  	 .addColumn("C_CREDIT_LIM", DataType.decimal())
							  	  	 .addColumn("C_DISCOUNT", DataType.decimal())							  	  	 
							  	  	 .addColumn("C_BALANCE", DataType.decimal())
							  	  	 .addColumn("C_YTD_PAYMENT", DataType.cfloat())
								 	 .addColumn("C_PAYMENT_CNT", DataType.cint())
								 	 .addColumn("C_DELIVERY_CNT", DataType.cint())
								 	 .addColumn("C_DATA", DataType.varchar());
		session.execute(create);
	}
	
	public static void createCustomerByBalanceView() {
		String query = 
				"CREATE MATERIALIZED VIEW IF NOT EXISTS customersbybalance AS " +
				"SELECT c_w_id, c_d_id, c_id, c_balance, c_first, c_middle, c_last " +
				"FROM customers " +
				"WHERE c_w_id IS NOT NULL " +
				  "AND c_balance IS NOT NULL " +
	              "AND c_d_id IS NOT NULL " +
	              "AND c_id IS NOT NULL " +
	            "PRIMARY KEY (c_balance, c_w_id, c_d_id, c_id) " +
                "WITH CLUSTERING ORDER BY (c_balance DESC);";
		session.execute(query);
	}
	
	public static void loadCustomerData(String filepath) {
		String[] columnNames = {"C_W_ID", "C_D_ID", "C_ID", "C_FIRST", "C_MIDDLE", 
								"C_LAST", "C_STREET_1", "C_STREET_2", "C_CITY", "C_STATE", 
								"C_ZIP", "C_PHONE", "C_SINCE", "C_CREDIT", "C_CREDIT_LIM",
								"C_DISCOUNT", "C_BALANCE", "C_YTD_PAYMENT", "C_PAYMENT_CNT",
								"C_DELIVERY_CNT", "C_DATA"};
		loadFromCsv("customers", columnNames, filepath);
	}
	
	public static void createOrderTable() {
		Create.Options create = SchemaBuilder.createTable("orders")
				                     .addPartitionKey("O_W_ID", DataType.cint())
				                     .addPartitionKey("O_D_ID", DataType.cint())
				                     .addClusteringColumn("O_ID", DataType.cint())
				                     .addColumn("O_C_ID", DataType.cint())
				                     .addColumn("O_CARRIER_ID", DataType.cint())
				                     .addColumn("O_OL_CNT", DataType.decimal())
				                     .addColumn("O_ALL_LOCAL", DataType.decimal())
				                     .addColumn("O_ENTRY_D", DataType.timestamp())
				                     .withOptions()
				                     .clusteringOrder("O_ID", SchemaBuilder.Direction.DESC);
		session.execute(create);
	}
	
	public static void loadOrderData(String filepath) {
		String[] columnNames = {"O_W_ID", "O_D_ID", "O_ID", "O_C_ID", "O_CARRIER_ID", 
								"O_OL_CNT", "O_ALL_LOCAL", "O_ENTRY_D"};
		loadFromCsv("orders", columnNames, filepath, "-1");
	}
	
	public static void createItemTable() {
		Create create = SchemaBuilder.createTable("items")
				                     .addPartitionKey("I_ID", DataType.cint())
				                     .addColumn("I_NAME", DataType.varchar())
				                     .addColumn("I_PRICE", DataType.decimal())
				                     .addColumn("I_IM_ID", DataType.cint())
				                     .addColumn("I_DATA", DataType.varchar());
		session.execute(create);
	}
	
	public static void loadItemData(String filepath) {
		String[] columnNames = {"I_ID", "I_NAME", "I_PRICE", "I_IM_ID", "I_DATA"};
		loadFromCsv("items", columnNames, filepath);
	}
	
	
	public static void createOrderLineTable() {
		Create create = SchemaBuilder.createTable("orderlines")
				                     .addPartitionKey("OL_W_ID", DataType.cint())
				                     .addPartitionKey("OL_D_ID", DataType.cint())
				                     .addClusteringColumn("OL_O_ID", DataType.cint())
				                     .addClusteringColumn("OL_NUMBER", DataType.cint())
				                     .addColumn("OL_I_ID", DataType.cint())
				                     .addColumn("OL_DELIVERY_D", DataType.timestamp())
				                     .addColumn("OL_AMOUNT", DataType.decimal())
				                     .addColumn("OL_SUPPLY_W_ID", DataType.cint())
				                     .addColumn("OL_QUANTITY", DataType.decimal())
				                     .addColumn("OL_DIST_INFO", DataType.varchar());
		session.execute(create);
	}
	
	public static void createOrderLineByItemView() {
		String query = 
				"CREATE MATERIALIZED VIEW IF NOT EXISTS orderlinesbyitem AS " +
				"SELECT ol_i_id, ol_w_id, ol_d_id, ol_o_id " +
				"FROM orderlines " +
				"WHERE ol_w_id IS NOT NULL AND ol_d_id IS NOT NULL " +
				"AND ol_o_id IS NOT NULL AND ol_i_id IS NOT NULL AND ol_number IS NOT NULL " +
				"PRIMARY KEY (ol_i_id, ol_w_id, ol_d_id, ol_o_id, ol_number);";
		session.execute(query);
	}
	
	public static void createRelatedOrderFunc() {
		String query = 
				"CREATE OR REPLACE FUNCTION state_group_and_count( state map<text, int>, w_id int, d_id int, o_id int ) " +
				"CALLED ON NULL INPUT " +
				"RETURNS map<text, int> " +
				"LANGUAGE java AS ' " +
				"String order = Integer.toString(w_id) + \",\" + Integer.toString(d_id) + \",\" + Integer.toString(o_id); " +
				"Integer count = (Integer) state.get(order); " +
				"if (count == null) count = 1; " +
				"else count++; state.put(order, count); " + 
				"return state;';";
		session.execute(query);
		query = 
				"CREATE OR REPLACE AGGREGATE relatedorder(int, int, int) " +
				"SFUNC state_group_and_count " +
				"STYPE map<text, int> " +
				"INITCOND {};";
		session.execute(query);
	}
	
	public static void loadOrderLineData(String filepath) {
		String[] columnNames = {"OL_W_ID", "OL_D_ID", "OL_O_ID", "OL_NUMBER", "OL_I_ID", 
								"OL_DELIVERY_D", "OL_AMOUNT", "OL_SUPPLY_W_ID", "OL_QUANTITY",
								"OL_DIST_INFO"};
		loadFromCsv("orderlines", columnNames, filepath);
	}
	
	public static void createStockTable() {
		Create create = SchemaBuilder.createTable("stock")
				                     .addPartitionKey("S_W_ID", DataType.cint())
				                     .addPartitionKey("S_I_ID", DataType.cint())
				                     .addColumn("S_QUANTITY", DataType.decimal())
				                     .addColumn("S_YTD", DataType.decimal())
				                     .addColumn("S_ORDER_CNT", DataType.cint())
				                     .addColumn("S_REMOTE_CNT", DataType.cint())
				                     .addColumn("S_DIST_01", DataType.varchar())
				                     .addColumn("S_DIST_02", DataType.varchar())
				                     .addColumn("S_DIST_03", DataType.varchar())
				                     .addColumn("S_DIST_04", DataType.varchar())
				                     .addColumn("S_DIST_05", DataType.varchar())
				                     .addColumn("S_DIST_06", DataType.varchar())
				                     .addColumn("S_DIST_07", DataType.varchar())
				                     .addColumn("S_DIST_08", DataType.varchar())
				                     .addColumn("S_DIST_09", DataType.varchar())
				                     .addColumn("S_DIST_10", DataType.varchar())
				                     .addColumn("S_DATA", DataType.varchar());
		session.execute(create);
	}
	
	public static void loadStockData(String filepath) {
		String[] columnNames = {"S_W_ID", "S_I_ID", "S_QUANTITY", "S_YTD", "S_ORDER_CNT", 
								"S_REMOTE_CNT", "S_DIST_01", "S_DIST_02", "S_DIST_03",
								"S_DIST_04", "S_DIST_05", "S_DIST_06", "S_DIST_07",
								"S_DIST_08", "S_DIST_09", "S_DIST_10", "S_DATA"};
		loadFromCsv("stock", columnNames, filepath);
	}
	
	private static void loadFromCsv(String tableName, String[] columnNames, String filepath) {
		String keyspace = session.getLoggedKeyspace();
		String columns = Arrays.toString(columnNames).replace("[", "(").replace("]", ")");
		String query = "COPY " + tableName + " " + columns + " FROM '" + filepath + "' " + 
					   "WITH NULL = 'null'";
		String[] cmd = {cqlshPath, "--keyspace", keyspace, "--execute", query};
		executeCmd(cmd);
	}
	
	private static void loadFromCsv(String tableName, String[] columnNames, String filepath, String replaceNull) {
		String keyspace = session.getLoggedKeyspace();
		String columns = Arrays.toString(columnNames).replace("[", "(").replace("]", ")");
		String query = "'COPY " + tableName + " " + columns + " FROM STDIN'";
		String sedCmd = "sed s/,null,/," + replaceNull + ",/ " + filepath;
		String cqlCmd =  cqlshPath + " --keyspace " + keyspace + " --execute " + query;
		String[] cmd = {"/bin/sh", "-c", sedCmd + " | " + cqlCmd};
		executeCmd(cmd);
	}
	
	private static void executeCmd(String[] cmd) {
		try {
			Process p = Runtime.getRuntime().exec(cmd);
			
			BufferedReader stdin = new BufferedReader(new InputStreamReader(p.getInputStream())); 
			BufferedReader stderr = new BufferedReader(new InputStreamReader(p.getErrorStream())); 
			String s = ""; 
			while ((s = stdin.readLine()) != null) { 
			     System.out.println(s); 
			} 
			while ((s = stderr.readLine()) != null) { 
			     System.out.println("ERROR: " + s); 
			} 
		}
		catch (Exception e) { 
			e.printStackTrace();
		} 
	}
}
