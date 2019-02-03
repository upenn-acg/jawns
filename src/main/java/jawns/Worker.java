package jawns;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.sqs.model.*;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import java.io.*;
import java.net.*;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static jawns.Jawns.*;
import static jawns.JobStatus.*;
import static jawns.WorkerCommand.*;

enum WorkerCommand { Run, Shutdown, JobCancelled }

public class Worker {

    private static final OptionSpec Help;
    private static final OptionSpec<String> Project;
    static final OptionSpec<Integer> PollSeconds;
    private static final OptionParser Parser;
    static OptionSet Options;

    private static String myHostname = "localhost";

    final static String SQS_MSG_GROUP_ID = "myGroupID";
    final static int SOCKET_PORT = 8888;

    volatile static WorkerCommand myCommand = Run;

    static {
        Parser = new OptionParser();
        Project = Parser.accepts("project", "project name")
                .withRequiredArg().ofType(String.class).required();
        PollSeconds = Parser.accepts("poll-seconds", "how long to wait (in seconds) between polling Job Table for work")
                .withRequiredArg().ofType(Integer.class).defaultsTo(20);
        Help = Parser.accepts("help", "Print this help message").forHelp();
    }

    public static void main(String[] args) {
        Options = Parser.parse(args);
        if (Options.has(Help)) {
            try {
                Parser.printHelpOn(System.out);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return;
        }

        // use a socket to ensure there's just one worker per node
        SocketThread st = new SocketThread();
        st.start();

        // determine our hostname
        try {
            ProcessOutput po = check_output(new ProcessBuilder("/bin/hostname","-f"));
            myHostname = po.stdout.trim() + po.stderr.trim();
        } catch (InterruptedException | IOException e) {
            LOG.severe(t2s(e,"couldn't determine hostname"));
        }

        Jawns J = new Jawns(Options.valueOf(Project));

        // listen on the Command Queue
        CommandQListenerThread comqThread = new CommandQListenerThread(J);
        comqThread.start();

        while (true) {
            getAndRunJob(J);
            switch (myCommand) {
                case Run:
                    sleep(Options.valueOf(PollSeconds), TimeUnit.SECONDS);
                    break;
                case Shutdown:
                    return;
                case JobCancelled:
                    myCommand = Run;
                    break;
                default:
                    LOG.severe("Invalid worker command: "+myCommand);
                    myCommand = Run;
                    break;
            }
        }

    }

    private static void getAndRunJob(Jawns J) {
        try {
            List<JobTableEntry> availableJobs = J.DBM.query(JobTableEntry.class, J.statusQuery(Available));
            // try to grab an Available job and run it
            for (JobTableEntry aj : availableJobs) {
                boolean changed = J.changeJobStatus(aj,Running);
                if (changed) {
                    aj.setJobStatus(Running);
                    runJob(J, aj);
                    return;
                }
            }
        } catch (AmazonClientException e) {
            LOG.warning(t2s(e,"error trying to find an Available job"));
            // try again later
        }
    }

    /** Runs this job, marking it in the Job Table as Succeeded or Failed as appropriate */
    private static void runJob(Jawns J, JobTableEntry job) {
        JobCancellationListenerThread jobCanceledThread = new JobCancellationListenerThread(J, job, Thread.currentThread());
        jobCanceledThread.start();

        ProcessBuilder pb = new ProcessBuilder("/bin/bash", "-c", job.getCommand());
        if (null != job.getCwd()) {
            pb.directory(new File(job.getCwd()));
        }
        try {

            // download pre-files from S3
            for (Map.Entry<String, File> sf : job.getS3PreFiles().entrySet()) {
                String s3ID = sf.getKey();
                File local = sf.getValue().isAbsolute() ? sf.getValue() : new File(pb.directory(), sf.getValue().getPath()); // NB: File ctor ignores 1st arg if null
                boolean dlok = J.downloadS3File(s3ID, local);
                if (!dlok) {
                    // if we can't download the pre-files, log the error and mark job as failed
                    String msg = String.format("couldn't download pre-file to '%s' (S3 ID: %s) for %s%n",
                            local, s3ID, job.name());
                    jobFailed(J, job, msg);
                    return;
                }
            }

            if (checkAndHandleJobCancellation(J, job)) { return; }

            // try to run the job
            Process proc = pb.start();
            StreamConsumer scOut = new StreamConsumer(J,job,true, myHostname,
                    proc.getInputStream(), new File(job.name()+".stdout"));
            StreamConsumer scErr = new StreamConsumer(J,job,false, myHostname,
                    proc.getErrorStream(), new File(job.name()+".stderr"));
            scOut.start();
            scErr.start();

            // wait for job to finish or hit timeout
            boolean[] timeoutHit = new boolean[] {false};
            TimeoutThread tot = null;
            if (job.getTimeoutSeconds() > 0) {
                // TimeoutThread kills the job when the timeout expires
                tot = new TimeoutThread(job.getTimeoutSeconds(), TimeUnit.SECONDS, proc, timeoutHit);
                tot.start();
            }
            int rc = Integer.MIN_VALUE;
            try {
                rc = proc.waitFor();
            } catch (InterruptedException e) {
                if (checkAndHandleJobCancellation(J, job)) {
                    proc.destroyForcibly();
                    Jawns.sleep(500, TimeUnit.MILLISECONDS); // wait for process to get cleaned up

                    WorkerResult wr = new WorkerResult(job);
                    wr.exitCode = proc.exitValue();
                    wr.jobTimedOut = false;
                    wr.type = WorkerResultType.JobFailed;
                    sendWorkerResult(J, wr);

                    return;
                }
            } finally {
                if (null != tot) {
                    tot.interrupt(); // tear down the TimeoutThread
                }
            }

            boolean jobSucceeded = (0 == rc) && !timeoutHit[0];

            if (!jobSucceeded) {
                WorkerResult wr = new WorkerResult(job);
                wr.exitCode = rc;
                wr.jobTimedOut = timeoutHit[0];
                wr.type = WorkerResultType.JobFailed;
                sendWorkerResult(J, wr);

                if (checkAndHandleJobCancellation(J, job)) {
                    return;
                }

                J.changeJobStatus(job, Failed);
                return;
            }

            // try to upload post-files to S3
            int fileNum = 0;
            for (Map.Entry<RemoteFile, File> wg : job.getPostFiles().entrySet()) {
                final File workerFile = wg.getKey();
                final File gatherFile = wg.getValue();
                if (!workerFile.canRead()) {
                    // if we can't upload the post-files, log the error and mark job as failed
                    String msg = String.format("couldn't read post-file '%s' for %s%n", workerFile, job.name());
                    jobFailed(J, job, msg);
                    return;
                }

                String key = String.format("job%d.post%d", job.getJobId(), fileNum);
                try {
                    J.S3.putObject(J.S3_BUCKET, key, workerFile);
                    job.getS3PostFiles().put(key, gatherFile);
                    fileNum++;
                } catch (AmazonClientException e) {
                    // if we can't upload the post-files, log the error and mark job as failed
                    String msg = String.format("couldn't upload post-file '%s' (S3ID: %s) for %s%n",
                            workerFile, key, job.name());
                    jobFailed(J, job, msg);
                    return;
                }
            }

            // send message on Result Queue
            WorkerResult wr = new WorkerResult(job);
            LOG.info(job.name());
            wr.exitCode = rc;
            wr.jobTimedOut = false;
            wr.type = WorkerResultType.JobSucceeded;
            sendWorkerResult(J, wr);

            // update Job Table
            J.changeJobStatus(job, Succeeded);
            //return;

        } catch (IOException e) {
            String msg = t2s(e, "exception when running "+job.name());
            LOG.severe(msg);
            jobFailed(J, job, msg);
        } finally {
            jobCanceledThread.interrupt(); // tear down the JobCancellationListenerThread
        }
    } // end runJob()

    /** @return true if our current Job has been cancelled, as signaled by the JobCancellationListenerThread */
    private static boolean checkAndHandleJobCancellation(Jawns J, JobTableEntry job) {
        JobTableEntry canceledJob = J.DBM.load(JobTableEntry.class, CancelRequested, job.getJobId());
        if (Thread.interrupted() || null != canceledJob) { // clears this thread's interruption state
            assert JobCancelled == myCommand : myCommand;
            canceledJob = J.DBM.load(JobTableEntry.class, CancelRequested, job.getJobId());
            assert null != canceledJob;
            J.changeJobStatus(canceledJob, Canceled);
            myCommand = Run;
            return true;
        }
        return false;
    }

    private static void sendWorkerResult(Jawns J, WorkerResult wr) {
        LOG.info("sending WorkerResult "+wr.type+" for "+wr.jobName());
        String jsonWR = GSON.toJson(wr);
        SendMessageRequest send = new SendMessageRequest()
                .withQueueUrl(J.RESULTQ_URL)
                .withMessageGroupId(Worker.SQS_MSG_GROUP_ID)
                .withMessageBody(jsonWR);
        try {
            J.SQS.sendMessage(send);
        } catch (AmazonClientException e) {
            LOG.severe(t2s(e, "error writing WorkerResult "+wr.type+" to result queue for "+wr.jobName()));
        }
    }

    private static void jobFailed(Jawns J, JobTableEntry job, String why) {
        // if we can't download the pre-files, log the error and mark job as failed
        LOG.severe(why);
        WorkerResult wr = new WorkerResult(job);
        wr.type = WorkerResultType.JobOutput;
        wr.startingByteIndex = 0;
        wr.output = why.getBytes();
        wr.workerHostname = myHostname;
        wr.outputType = JobOutputType.JobLog;
        sendWorkerResult(J, wr);

        // send JobFailure on Result Queue
        wr = new WorkerResult(job);
        wr.exitCode = Integer.MAX_VALUE;
        wr.type = WorkerResultType.JobFailed;
        sendWorkerResult(J, wr);

        J.changeJobStatus(job, Failed);
    }

}

/** Consumes stdout/stderr from a process, writing it to disk and the Result Queue */
class StreamConsumer extends Thread {

    final private BufferedInputStream istream;
    /** a local file to write the stream to, in addition to sending it via Result Queue */
    final private File localFile;
    final private Jawns J;
    /** if true, this stream is a job's stdout, false if stderr */
    final private boolean stdout;
    final private JobTableEntry myJob;
    final private String myHostname;

    final private byte[] buf = new byte[8192];

    StreamConsumer(Jawns j, JobTableEntry job, boolean isStdout, String myHost, InputStream is, File f) {
        J = j;
        istream = new BufferedInputStream(is);
        localFile = f;
        stdout = isStdout;
        myJob = job;
        myHostname = myHost;
    }

    private int totalBytesRead = 0;

    @Override
    public void run() {
        while (true) {
            try {
                int bytesRead = istream.read(buf);
                if (-1 == bytesRead) { // EOF
                    return;
                }

                // write output to local file
                try (FileOutputStream fos = new FileOutputStream(localFile)) {
                    fos.write(buf, 0, bytesRead);
                } catch (IOException ioe) {
                    LOG.warning(t2s(ioe,"problem writing to local file for "+myJob.name()));
                }

                // send output to result queue
                WorkerResult wr = new WorkerResult(myJob);
                wr.type = WorkerResultType.JobOutput;
                wr.startingByteIndex = totalBytesRead;
                totalBytesRead += bytesRead;
                wr.output = Arrays.copyOfRange(buf, 0, bytesRead);
                wr.workerHostname = myHostname;
                wr.outputType = stdout ? JobOutputType.JobStdout : JobOutputType.JobStderr;
                String jsonWR = GSON.toJson(wr);
                LOG.info("sending WorkerResult: "+jsonWR);

                SendMessageRequest send = new SendMessageRequest()
                        .withQueueUrl(J.RESULTQ_URL)
                        .withMessageGroupId(Worker.SQS_MSG_GROUP_ID)
                        .withMessageBody(jsonWR);
                try {
                    J.SQS.sendMessage(send);
                } catch (AmazonClientException e) {
                    LOG.severe(t2s(e, "error writing stdout/stderr to result queue for "+myJob.name()));
                }


            } catch (IOException ioe) {
                LOG.severe(t2s(ioe,"StreamConsumer: error processing stream for "+myJob.name()));
                return;
            }
        }
    }
}

/** A thread that sleeps for the given amount of time and then interrupts the target Thread */
class TimeoutThread extends Thread {

    final private long millisToSleep;
    final private Process process;
    final private boolean[] timedOut;

    /**
     * @param duration the timeout lasts `duration` `units` of time
     * @param units the timeout lasts `duration` `units` of time
     * @param processToKill the Process to kill when the timeout expires
     * @param hitTimeout a (1-element array) output parameter that indicates whether the timeout expired or not
     */
    TimeoutThread(long duration, TimeUnit units, Process processToKill, boolean[] hitTimeout) {
        millisToSleep = units.toMillis(duration);
        process = processToKill;
        assert 1 == hitTimeout.length;
        timedOut = hitTimeout;
        timedOut[0] = false;
        this.setDaemon(true); // don't delay exit of the worker process
    }

    @Override
    public void run() {
        try {
            Thread.sleep(millisToSleep);
            LOG.info("timeout expired, killing process");
            timedOut[0] = true;
            process.destroyForcibly();
        } catch (InterruptedException e) {
            // If we get interrupted, it's because the job completed and we aren't needed anymore,
            // so just exit quietly
        }
    }
}

class CommandQListenerThread extends Thread {

    final private Jawns J;

    CommandQListenerThread(Jawns jawns) {
        this.J = jawns;
        this.setDaemon(true);
    }

    @Override
    public void run() {
        while (true) {
            Jawns.sleep(Worker.Options.valueOf(Worker.PollSeconds), TimeUnit.SECONDS);

            ReceiveMessageResult res;
            try {
                res = J.SQS.receiveMessage(J.COMMANDQ_URL);
            } catch (AmazonClientException e) {
                LOG.severe(t2s(e, "error receiving Command Queue message"));
                continue;
            }

            for (Message msg : res.getMessages()) {
                WorkerRequest wsr = GSON.fromJson(msg.getBody(), WorkerRequest.class);
                LOG.info("Received WorkerRequest: " + msg.getBody());
                final String messageReceiptHandle = msg.getReceiptHandle();
                try {
                    J.SQS.deleteMessage(new DeleteMessageRequest(J.COMMANDQ_URL, messageReceiptHandle));
                } catch (AmazonClientException e) {
                    LOG.severe(t2s(e, "error deleting Command Queue message"));
                }

                if (WorkerStatusRequestType.WorkerShutdown == wsr.type) {
                    Worker.myCommand = Shutdown;
                    return;
                } else {
                    LOG.severe("invalid WorkerRequest: " + wsr);
                }

            }
        }
    }
}

class JobCancellationListenerThread extends Thread {

    final private Jawns J;
    final private Thread workerT;
    final private JobTableEntry jte;

    JobCancellationListenerThread(Jawns jawns, JobTableEntry job, Thread workerThread) {
        this.J = jawns;
        this.jte = job;
        this.workerT = workerThread;
        this.setDaemon(true);
    }

    @Override
    public void run() {
        while (true) {
            Jawns.sleep(Worker.Options.valueOf(Worker.PollSeconds), TimeUnit.SECONDS);

            JobTableEntry canceledJob = J.DBM.load(JobTableEntry.class, CancelRequested, jte.getJobId());
            if (null != canceledJob) {
                LOG.info("Cancellation request received for "+jte.name());
                Worker.myCommand = JobCancelled;
                workerT.interrupt();
            }
            if (Thread.interrupted()) {
                return;
            }
        }
    }
}

class SocketThread extends Thread {

    SocketThread() {
        this.setDaemon(true);
    }

    @Override
    public void run() {
        byte[] incomingMsg = new byte[1024];

        try (ServerSocket serverSocket = new ServerSocket(Worker.SOCKET_PORT, 1, InetAddress.getByName("127.0.0.1"))) {
            serverSocket.setSoTimeout(60 * 1000); // 1m
            while (true) {
                try {
                    Socket sock = serverSocket.accept();
                    int bytesRead = sock.getInputStream().read(incomingMsg);
                    LOG.info("Socket message received: "+new String(incomingMsg,0,bytesRead));
                } catch (SocketTimeoutException e) { }
            }

        } catch (BindException e) {
            LOG.warning(t2s(e,"Another Jawns worker is already running, exiting."));
            System.exit(1);
        } catch (IOException e) {
            LOG.severe(t2s(e,"error listening on socket. Exiting."));
            System.exit(2);
        }
    }
}