import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.Session;
import com.google.common.math.Quantiles;

public class SingleDriver {
    private static int index;
    private static String serverIP = "192.168.56.159";
    private static String keyspace = "cs5424";
    private static ConsistencyLevel readConsistencyLevel;
    private static ConsistencyLevel writeConsistencyLevel;
    private static Session session;
    private static Cluster cluster;
    private static String xactDir = "/temp/project-files/xact-files/";
    private static String xactFilepath;
    private static int clientCount;
    private static String readConsistency="", writeConsistency="";
    private static String outFolder = "output/transactions/";
    private static String errFolder = "output/performance/";
    private static List<String> serverIPs =  new ArrayList<>(Arrays.asList("192.168.56.159","192.168.56.160","192.168.56.161","192.168.56.162","192.168.56.163"));

    public static void main(String[] args) {
        if(args.length < 3) {
            System.out.println("3 command line arguments expected - index, ReadConsistency, WriteConsistency...");
            System.exit(2);
        }else if(args.length == 3){
            index = Integer.parseInt(args[0]);
            readConsistency = args[1];
            readConsistencyLevel = getConsistencyLevel(readConsistency);
            writeConsistency = args[2];
            writeConsistencyLevel = getConsistencyLevel(writeConsistency);
            xactFilepath = xactDir + "/" + index + ".txt";
        }
        else{
            System.out.println("Wrong number of command line arguments - expected 3 argument - index, ReadConsistency, WriteConsistency...");
            System.exit(2);
        }
        serverIP = serverIPs.get(index%5);
        System.out.println("Running: "+index+" index with readConsistency: "+readConsistency+" writeConsistency"+writeConsistency+" in ip:"+serverIP);

        //setOut("_stdout.txt", true);
        startSession();

        Pair<Long, Long> transactionOp = readXactFile();
        Long numOfTransactions = transactionOp.getKey();
        Long execTimeMs = transactionOp.getValue();

        closeSession();
        Double execTimeSec = (double) execTimeMs / 1000.0;
        System.out.close();
    }

    public static Pair<Long, Long> readXactFile() {
        //String xactFilepath = xactDir + "/" + xactFileId + ".txt";
        try {
            Scanner sc = new Scanner(new File(xactFilepath));
            Pair<Long, Long> transacOp = readTransactions(sc);
            sc.close();
            return transacOp;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return new Pair<>(0L,0L);
    }

    private static Pair<Long, Long> readTransactions(Scanner sc) throws FileNotFoundException {
        long count = 0;
        List<Long> transactionTimeList = new ArrayList<>();
        Long totalTime = 0L;
        long startTime, endTime;
        TransactionFactory factory = new TransactionFactory(session, writeConsistencyLevel);
        while (sc.hasNext()) {
            count++;
            startTime = System.currentTimeMillis();
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
            endTime = System.currentTimeMillis();
            transactionTimeList.add(endTime-startTime);
            totalTime += (endTime-startTime);
        }
        Double averageTime = (double) totalTime / (double) count;
        Collections.sort(transactionTimeList);
        double median = Quantiles.median().compute(transactionTimeList);
        double percentile95 = Quantiles.percentiles().index(95).compute(transactionTimeList);
        double percentile99 = Quantiles.percentiles().index(99).compute(transactionTimeList);
        Double execTimeSec = (double) totalTime / 1000.0;
        //setOut("_stderr.txt", false);
        System.err.println("Number of executed transaction is: "+count);
        System.err.println("Total transaction execution time (in seconds) is: "+execTimeSec);
        System.err.println("Transaction throughput is: "+(double) count / (double) execTimeSec);
        System.err.println("Average transaction latency in ms is: "+averageTime);
        System.err.println("Median is :"+median);
        System.err.println("95 Percentile is: "+percentile95);
        System.err.println("99 Percentile is: "+percentile99);
        // Std err - averageTime, median, percentile95 and percentile 99
        return new Pair<>(count, totalTime);
    }

    public static void startSession() {
        cluster = Cluster.builder()
                .addContactPoints(serverIP)
                .withQueryOptions(new QueryOptions().setConsistencyLevel(readConsistencyLevel))
                .build();
        session = cluster.connect(keyspace);
    }

    private static ConsistencyLevel getConsistencyLevel(String name) {
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

    public static void closeSession() {
        session.close();
        cluster.close();
    }
}
