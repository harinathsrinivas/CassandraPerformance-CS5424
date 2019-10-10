import java.math.BigDecimal;
import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.ReadTimeoutException;

public class DatabaseState {
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
                System.out.println("sum(W YTD) \tsum(D_NEXT_O_ID)");
                System.out.println(row.getDecimal(0) + "\t" +  row.getInt(1));

                String query3 = "select sum(C_BALANCE), sum(C_YTD_PAYMENT), sum(C_PAYMENT_CNT), sum(C_DELIVERY_CNT) from Customers;";
                rs = session.execute(query3);
                row = rs.one();
                System.out.println("Query 3");
                System.out.println("sum(C_BALANCE) \tsum(C_YTD_PAYMENT) \tsum(C_PAYMENT_CNT) \tsum(C_DELIVERY_CNT)");
                System.out.println(row.getDecimal(0) + "\t" +  row.getFloat(1) + "\t\t" + row.getInt(2) + "\t\t\t" + row.getInt(3));

                String query4 = "select max(O_ID), sum(O_OL_CNT) from Orders;";
                rs = session.execute(query4);
                row = rs.one();
                System.out.println("Query 4");
                System.out.println("max(O_ID) \tsum(O_OL_CNT)");
                System.out.println(row.getInt(0) + "\t\t" + row.getDecimal(1));

                // Query 5
                BigDecimal OL_AMOUNT= new BigDecimal(0);
                BigDecimal OL_QUANTITY= new BigDecimal(0);

                for(int i=1; i<=10; i++)
                    for(int j=1; j<=10; j++){
                        String query5 = "select sum(OL_AMOUNT), sum(OL_QUANTITY) from OrderLines where OL_W_ID ="+i+" and OL_D_ID="+j+";";
                        rs = session.execute(query5);
                        row = rs.one();
                        OL_AMOUNT = OL_AMOUNT.add(row.getDecimal(0));
                        OL_QUANTITY = OL_QUANTITY.add(row.getDecimal(1));
                        //System.out.println("For w_id"+i+" d_id"+j+" OL_AMT"+OL_AMOUNT+" OL_QTY"+OL_QUANTITY);
                    }
                System.out.println("Query 5");
                System.out.println("sum(OL_AMOUNT) \tsum(OL_QUANTITY)");
                System.out.println(OL_AMOUNT + "\t\t" + OL_QUANTITY);


                // Query 6
                BigDecimal S_QUANTITY= new BigDecimal(0);
                BigDecimal S_YTD= new BigDecimal(0);
                int S_ORDER_CNT = 0;
                int S_REMOTE_CNT = 0;
                for(int i=1; i<=10; i++)
                    for(int j1=0, j2=3000; j2<=100000; j1+=3000, j2+=3000){
                        String query6;
                        if(j2 != 100000)
                            query6 = "select sum(S_QUANTITY), sum(S_YTD), sum(S_ORDER_CNT), sum(S_REMOTE_CNT) from Stock where s_w_id = "+i+" and s_i_id >= "+j1+" and s_i_id < "+j2+" ALLOW FILTERING;";
                        else
                            query6 = "select sum(S_QUANTITY), sum(S_YTD), sum(S_ORDER_CNT), sum(S_REMOTE_CNT) from Stock where s_w_id = "+i+" and s_i_id >= "+j1+" and s_i_id <= "+j2+" ALLOW FILTERING;";

                        rs = session.execute(query6);
                        row = rs.one();
                        S_QUANTITY = S_QUANTITY.add(row.getDecimal(0));
                        S_YTD = S_YTD.add(row.getDecimal(1));
                        S_ORDER_CNT += row.getInt(2);
                        S_REMOTE_CNT += row.getInt(3);
                    }

                System.out.println("Query 6");
                System.out.println("sum(S_QUANTITY) \tsum(S_YTD) \tsum(S_ORDER_CNT) \tsum(S_REMOTE_CNT)");
                System.out.println(S_QUANTITY + "\t\t" + S_YTD + "\t\t" + S_ORDER_CNT + "\t\t\t" + S_REMOTE_CNT);
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