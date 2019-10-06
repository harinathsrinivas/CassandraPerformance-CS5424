import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.Session;

public class DriverOld {
    private String serverIP = "192.168.56.159";
    private String keyspace = "cs5424";
    private ConsistencyLevel readConsistencyLevel;
    private ConsistencyLevel writeConsistencyLevel;
    private Session session;
    private Cluster cluster;

    public static void main(String[] args) {
        String xactDir = "/temp/project-files/xact-files/";
        DriverOld driver = new DriverOld();
        driver.startSession("ONE", "ONE");
        long numOfTransactions = driver.readXactFile(1, xactDir);// executes 'project-files/xact-files/1.txt'
        driver.closeSession();
        System.out.println("Number of transactions are "+numOfTransactions);
    }

    public long readXactFile(int xactFileId, String xactDir) {
        String xactFilepath = xactDir + "/" + xactFileId + ".txt";
        try {
            Scanner sc = new Scanner(new File(xactFilepath));
            long xactCount = readTransactions(sc);
            sc.close();
            return xactCount;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return 0;
    }

    private long readTransactions(Scanner sc) {
        long count = 0;
        TransactionFactory factory = new TransactionFactory(session, writeConsistencyLevel);
        while (sc.hasNext()) {
            count++;
            String[] args = sc.nextLine().split(",");
            String type = args[0];
            if (type.equals("N")) {

                int n = Integer.parseInt(args[args.length - 1]);
                List<String> orderLines = new ArrayList<String>();
                for (int i = 0; i < n; i++) {
                    orderLines.add(sc.nextLine());
                }
                String csv = String.join("\n", orderLines);

                List<String> argsList = new ArrayList<String>();
                for (String arg: args) {
                    argsList.add(arg);
                }
                argsList.add(csv);
                args = argsList.toArray(new String[argsList.size()]);
            }
            Transaction transaction = factory.getTransaction(type);
            transaction.process(args);
        }
        return count;
    }

    public void startSession(String readConsistency, String writeConsistency) {
        readConsistencyLevel = getConsistencyLevel(readConsistency);
        writeConsistencyLevel = getConsistencyLevel(writeConsistency);

        cluster = Cluster.builder()
                .addContactPoints(serverIP)
                .withQueryOptions(new QueryOptions().setConsistencyLevel(readConsistencyLevel))
                .build();
        session = cluster.connect(keyspace);
    }

    private ConsistencyLevel getConsistencyLevel(String name) {
        switch (name) {
            case "QUORUM":
                return ConsistencyLevel.QUORUM;
            case "ALL":
                return ConsistencyLevel.ALL;
            case "ONE":
                return ConsistencyLevel.ONE;
            default:
                return ConsistencyLevel.ONE;
        }
    }

    public void closeSession() {
        session.close();
        cluster.close();
    }
}