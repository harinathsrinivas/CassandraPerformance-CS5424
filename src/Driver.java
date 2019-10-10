import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.io.FileNotFoundException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.Session;
import com.google.common.math.Quantiles;
import com.google.common.math.Stats;

public class Driver implements Callable<Pair<Long, Double>> {
	private int index;
	private String serverIP = "192.168.56.159";
	private String keyspace = "cs5424";
	private ConsistencyLevel readConsistencyLevel;
	private ConsistencyLevel writeConsistencyLevel;
	private Session session;
	private Cluster cluster;
	private String xactDir = "/temp/project-files/xact-files/";
	private String xactFilepath;
	private static int clientCount;
	private static String readConsistency="", writeConsistency="";
	private static String outFolder = "output/transactions/";
	private static String errFolder = "output/performance/";



	Driver(int idx, String readConsistency, String writeConsistency, String ip) {
		this.index = idx;
		this.readConsistencyLevel = getConsistencyLevel(readConsistency);
		this.writeConsistencyLevel = getConsistencyLevel(writeConsistency);
		this.serverIP = ip;
		this.xactFilepath = xactDir + "/" + index + ".txt";
	}
	private void setOut(String filenNameEnd, boolean isOut) throws FileNotFoundException {
		File file;
		if(isOut){
			file = new File(outFolder+index+filenNameEnd);}
		else{
			file = new File(errFolder+index+filenNameEnd);}

		FileOutputStream fos = new FileOutputStream(file);
		PrintStream ps = new PrintStream(fos);
		((ThreadPrintStream)System.out).setThreadOut(ps);
	}
	@Override
	public Pair<Long, Double> call() throws Exception {
		File file = new File(index+"_stdout.txt");
		FileOutputStream fos = new FileOutputStream(file);
		PrintStream ps = new PrintStream(fos);
		//System.setOut(ps);
		((ThreadPrintStream)System.out).setThreadOut(ps);
		this.setOut("_stdout.txt", true);
		startSession();

		Pair<Long, Long> transactionOp = readXactFile();
		Long numOfTransactions = transactionOp.getKey();
		Long execTimeMs = transactionOp.getValue();
		closeSession();
		//System.out.println("Number of transactions in thread: "+numOfTransactions);
		//System.out.println("Time taken in ms: "+execTimeMs);
		Double execTimeSec = (double) execTimeMs / 1000.0;
		//System.out.println("Time taken in secs: "+execTimeSec);
		// Close System.out for this thread which will
		// flush and close this thread's text file.
		System.out.close();
		return new Pair<>(numOfTransactions, execTimeSec);
		//return numOfTransactions;
	}


	public static void main(String[] args) throws InterruptedException {

		if(args.length < 3) {
			System.out.println("3 command line arguments expected - NumberOfClients, ReadConsistency, WriteConsistency...");
			System.exit(2);
		}else if(args.length == 3){
			clientCount = Integer.parseInt(args[0]);
			readConsistency = args[1];
			writeConsistency = args[2];
		}
		else{
			System.out.println("Wrong number of command line arguments - expected 3 argument - NumberOfClients, ReadConsistency, WriteConsistency...");
			System.exit(2);
		}
		/*Driver driver = new Driver(1, "ONE", "ONE", "192.168.56.159");
		driver.startSession("ONE", "ONE");
		long numOfTransactions = driver.readXactFile(1, xactDir);// executes 'project-files/xact-files/1.txt'*/
		new File(outFolder).mkdirs();
		new File(errFolder).mkdirs();

		List<String> serverIPs =  new ArrayList<>(Arrays.asList("192.168.56.159","192.168.56.160","192.168.56.161","192.168.56.162","192.168.56.163"));
		//Scanner sc = new Scanner(System.in);
		System.out.println("Enter the number of Clients (NC) to execute (1<= NC <=40): ");
		//int clientCount = sc.nextInt();
		//int clientCount = 10;
		System.out.println("Running: "+clientCount+" clients with readConsistency: "+readConsistency+" writeConsistency"+writeConsistency);
		/*System.out.println("Select one of the following consistency levels...");
		System.out.println("(1) Write - QUORUM, Read - QUORUM");
		System.out.println("(2) Write - ALL, Read - ONE");
		System.out.println("Enter your option: ");*/
		//int consOpt = sc.nextInt();
		//int consOpt = 1;
		/*if(consOpt == 1){
			writeConsistency = "QUORUM";
			readConsistency = "QUORUM";
		}
		else if(consOpt == 2){
			writeConsistency = "ALL";
			readConsistency = "ONE";
		}
		else if(consOpt == 3){
			writeConsistency = "ONE";
			readConsistency = "ONE";
		}
		else{
			System.out.println("Invalid option selected exiting..");
			System.exit(2);
		}*/
		ExecutorService executorService = Executors.newFixedThreadPool(clientCount);
		//List<Future<Long>> futureList = new ArrayList<>(clientCount);
		List<Callable<Pair<Long, Double>>> callableList = new ArrayList<>(clientCount);
		//sc.close();
		ThreadPrintStream.replaceSystemOut();
		for(int i=1; i<=clientCount; i++){
			System.out.println("index is "+i+" ip is "+serverIPs.get(i%5));
			Callable<Pair<Long, Double>> callable = new Driver(i, readConsistency, writeConsistency, serverIPs.get(i%5));
			callableList.add(callable);
			//Future<Long> future = executorService.submit(callable);
			//futureList.add(future);
		}
		List<Future<Pair<Long, Double>>> futureList = executorService.invokeAll(callableList);
		for(Future<Pair<Long, Double>> fut : futureList){
			try {
				//print the return value of Future, notice the output delay in console
				// because Future.get() waits for task to get completed
				Pair<Long, Double> resultPair = fut.get();
				System.out.println("Number of transactions is ::"+resultPair.getKey());
				System.out.println("Exec time in ms ::"+resultPair.getValue());
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
			}
		}
		//shut down the executor service now
		executorService.shutdown();
	}

	public Pair<Long, Long> readXactFile() {
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
	
	private Pair<Long, Long> readTransactions(Scanner sc) throws FileNotFoundException {
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
		//double median;
		double mean = Stats.meanOf(transactionTimeList);
		double median = Quantiles.median().compute(transactionTimeList);
		double percentile95 = Quantiles.percentiles().index(95).compute(transactionTimeList);
		double percentile99 = Quantiles.percentiles().index(99).compute(transactionTimeList);
		Double execTimeSec = (double) totalTime / 1000.0;
		System.out.close();
		this.setOut("_stderr.txt", false);
		System.out.println("Number of executed transaction is: "+count);
		System.out.println("Total transaction execution time (in seconds) is: "+execTimeSec);
		System.out.println("Transaction throughput is: "+(double) count / (double) execTimeSec);
		System.out.println("Average transaction latency in ms is: "+mean);
		System.out.println("Median is :"+median);
		System.out.println("95 Percentile is: "+percentile95);
		System.out.println("99 Percentile is: "+percentile99);
		// Std err - averageTime, median, percentile95 and percentile 99
		return new Pair<>(count, totalTime);
    }
	
	public void startSession() {
		//readConsistencyLevel = getConsistencyLevel(readConsistency);
		//writeConsistencyLevel = getConsistencyLevel(writeConsistency);
		
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

class Pair<K,V> {
	private V value;
	private K key;

	public K getKey() { return key; }
	public V getValue() { return value; }

	public Pair(K key, V value) {
		this.key = key;
		this.value = value;
	}
}
