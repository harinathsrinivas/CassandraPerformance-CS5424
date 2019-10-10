import java.io.*;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;

public class MultiProcessDriver implements Callable<Boolean> {
    private int index;
    private static int clientCount;
    private static String readConsistency="", writeConsistency="";
    private static String outFolder = "output/transactions/";
    private static String errFolder = "output/performance/";
    private Process extprocess = null;


    MultiProcessDriver(int idx, String readCon, String writeCon) {
        this.index = idx;
        this.readConsistency = readCon;
        this.writeConsistency = writeCon;
    }

    @Override
    public Boolean call() {
        try {
            System.out.println("Calling new process for index: "+index+" readConsistency: "+readConsistency+" writeConsistency: "+writeConsistency);
            ProcessBuilder pb = new ProcessBuilder("java", "-jar", "SingleDriver.jar", String.valueOf(index), readConsistency, writeConsistency);
            //Process p = pb.start();
            File dirOut = new File(outFolder+index+"_stdout.txt");
            File dirErr = new File(errFolder+index+"_stderr.txt");
            pb.redirectOutput(dirOut);
            pb.redirectError(dirErr);
            Process extprocess = pb.start();

            extprocess.waitFor();
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        } catch (InterruptedException e) {
            e.printStackTrace();
            return false;
        }
        return true;
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

        new File(outFolder).mkdirs();
        new File(errFolder).mkdirs();

        ExecutorService executorService = Executors.newFixedThreadPool(clientCount);
        //List<Future<Long>> futureList = new ArrayList<>(clientCount);
        //List<Runnable> runnableList = new ArrayList<>(clientCount);
        List<Callable<Boolean>> callableList = new ArrayList<>(clientCount);
        //sc.close();
        for(int i=1; i<=clientCount; i++){
            Callable<Boolean> callable = new MultiProcessDriver(i, readConsistency, writeConsistency);
            callableList.add(callable);
        }
        List<Future<Boolean>> futureList = executorService.invokeAll(callableList);
        for(Future<Boolean> fut : futureList){
            try {
                //print the return value of Future, notice the output delay in console
                // because Future.get() waits for task to get completed
                Boolean b = fut.get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }
        //shut down the executor service now
        executorService.shutdown();
    }
}
