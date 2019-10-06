import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.Session;

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

	Driver(int idx, String readConsistency, String writeConsistency, String ip) {
		this.index = idx;
		this.readConsistencyLevel = getConsistencyLevel(readConsistency);
		this.writeConsistencyLevel = getConsistencyLevel(writeConsistency);
		this.serverIP = ip;
		this.xactFilepath = xactDir + "/" + index + ".txt";
	}
	@Override
	public Pair<Long, Double> call() throws Exception {
		File file = new File(index+"_stdout.txt");
		FileOutputStream fos = new FileOutputStream(file);
		PrintStream ps = new PrintStream(fos);
		//System.setOut(ps);
		((ThreadPrintStream)System.out).setThreadOut(ps);
		long startTime = System.currentTimeMillis();
		startSession();
		long numOfTransactions = readXactFile();
		closeSession();
		long endTime = System.currentTimeMillis();
		System.out.println("Number of transactions in thread: "+numOfTransactions);
		System.out.println("Time taken in ms: "+(endTime-startTime));
		Double execTime = (double) (endTime - startTime) / 1000.0;
		System.out.println("Time taken in secs: "+execTime);
		// Close System.out for this thread which will
		// flush and close this thread's text file.
		System.out.close();
		return new Pair<>(numOfTransactions, execTime);
		//return numOfTransactions;
	}

	public static void main(String[] args) throws InterruptedException {

		/*Driver driver = new Driver(1, "ONE", "ONE", "192.168.56.159");
		driver.startSession("ONE", "ONE");
		long numOfTransactions = driver.readXactFile(1, xactDir);// executes 'project-files/xact-files/1.txt'*/

		List<String> serverIPs =  new ArrayList<>(Arrays.asList("192.168.56.159","192.168.56.160","192.168.56.161","192.168.56.162","192.168.56.163"));
		Scanner sc = new Scanner(System.in);
		System.out.println("Enter the number of Clients (NC) to execute (1<= NC <=40): ");
		int clientCount = sc.nextInt();
		String readConsistency="", writeConsistency="";
		System.out.println("Select one of the following consistency levels...");
		System.out.println("(1) Write - QUORUM, Read - QUORUM");
		System.out.println("(2) Write - ALL, Read - ONE");
		System.out.println("Enter your option: ");
		int consOpt = sc.nextInt();
		if(consOpt == 1){
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
		}
		ExecutorService executorService = Executors.newFixedThreadPool(5);
		//List<Future<Long>> futureList = new ArrayList<>(clientCount);
		List<Callable<Pair<Long, Double>>> callableList = new ArrayList<>(clientCount);
		sc.close();
		ThreadPrintStream.replaceSystemOut();
		for(int i=1; i<=clientCount; i++){
			System.out.println("index is "+i+" ip is"+serverIPs.get(i%5)+ " clientCount is "+clientCount);
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

	public long readXactFile() {
		//String xactFilepath = xactDir + "/" + xactFileId + ".txt";
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
