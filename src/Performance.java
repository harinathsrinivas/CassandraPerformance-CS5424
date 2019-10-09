import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.ReadTimeoutException;

public class Performance {
    private static String cqlshPath = "/temp/Cassandra/bin/cqlsh";
    private static String serverIP = "192.168.56.159";
    private static String keyspace = "cs5424";
    private static Session session;
    private static Cluster cluster;

    public static void main(String[] args) {
        if(args.length < 1) {
            System.out.println("1 command line arguments expected - Ip address of Running node...");
            System.exit(2);
        }else if(args.length == 1){
            serverIP = args[0];
            System.out.println("Running the program in IP: "+serverIP);
        }
        else{
            System.out.println("Wrong number of command line arguments - expected 1 argument - Ip address...");
            System.exit(2);
        }
        // Use existing keyspace and create session
        useKeyspace(keyspace);
        // Execute performance queries
        executePerformanceQueries(keyspace);
        //Close session
        closeSession();
    }

    public static void useKeyspace(String keyspace) {
        cluster = Cluster.builder()
                .addContactPoints(serverIP)
                .build();
        session = cluster.connect(keyspace);
    }
    public static void executePerformanceQueries(String keyspaceName) {
        List<String> queryList = new ArrayList<>();
        int retry = 10;
        while(true && (retry--)>0) {
            try {
                String query1 = "select sum(w_ytd) from warehouses;";
                ResultSet rs = session.execute(query1);
                Row row = rs.one();
                System.out.println("Query 1");
                System.out.println("sum(W_YTD)");
                System.out.println(row.getDecimal(0));

                String query2 = "select sum(D_YTD), sum(D_NEXT_O_ID) from Districts;";
                rs = session.execute(query2);
                row = rs.one();
                System.out.println("Query 2");
                System.out.println("sum(W YTD) \t sum(D_NEXT_O_ID)");
                System.out.println(row.getDecimal(0) + "\t" + row.getInt(1));
                queryList.add(query2);

                String query3 = "select sum(C_BALANCE), sum(C_YTD_PAYMENT), sum(C_PAYMENT_CNT), sum(C_DELIVERY_CNT) from Customers;";
                rs = session.execute(query3);
                row = rs.one();
                System.out.println("Query 3");
                System.out.println("sum(C_BALANCE) \t sum(C_YTD_PAYMENT) \t sum(C_PAYMENT_CNT) \t sum(C_DELIVERY_CNT)");
                System.out.println(row.getDecimal(0) + "\t" + row.getFloat(1) + "\t" + row.getInt(2) + "\t" + row.getInt(3));
                queryList.add(query3);

                String query4 = "select max(O_ID), sum(O_OL_CNT) from Orders;";
                rs = session.execute(query4);
                row = rs.one();
                System.out.println("Query 4");
                System.out.println("max(O_ID) \t sum(O_OL_CNT)");
                System.out.println(row.getInt(0) + "\t" + row.getDecimal(1));
                queryList.add(query4);

                String query5 = "select sum(OL_AMOUNT), sum(OL_QUANTITY) from OrderLines;";
                rs = session.execute(query5);
                row = rs.one();
                System.out.println("Query 5");
                System.out.println(" sum(OL_AMOUNT) \t  sum(OL_QUANTITY)");
                System.out.println(row.getDecimal(0) + "\t" + row.getDecimal(1));
                queryList.add(query5);

                String query6 = "select sum(S_QUANTITY), sum(S_YTD), sum(S_ORDER_CNT), sum(S_REMOTE_CNT) from Stock;";
                rs = session.execute(query6);
                row = rs.one();
                System.out.println("Query 6");
                System.out.println(" sum(S_QUANTITY) \t sum(S_YTD) \t sum(S_ORDER_CNT) \t sum(S_REMOTE_CNT)");
                System.out.println(row.getDecimal(0) + "\t" + row.getDecimal(1) + "\t" + row.getInt(2) + "\t" + row.getInt(3));
                queryList.add(query6);
                break;
            } catch (ReadTimeoutException e) {
                e.printStackTrace();
                System.out.println("Some query failed because of consistency or response from other replicas.. retrying");
                continue;
            }
        }
        if(retry == 0)
            System.out.println("10 retries done, check all the replicas and run the jar again later.. exiting");

    }
    public static void closeSession() {
        session.close();
        cluster.close();
    }
}
