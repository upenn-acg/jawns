package jawns;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.model.*;
import com.amazonaws.services.dynamodbv2.util.TableUtils;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.simpleemail.AmazonSimpleEmailService;
import com.amazonaws.services.simpleemail.AmazonSimpleEmailServiceClientBuilder;
import com.amazonaws.services.simpleemail.model.Body;
import com.amazonaws.services.simpleemail.model.Content;
import com.amazonaws.services.simpleemail.model.Destination;
import com.amazonaws.services.simpleemail.model.SendEmailRequest;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.*;
import com.google.gson.Gson;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.*;
import java.util.stream.Collectors;

import static jawns.JobStatus.*;
import static jawns.WorkerStatusRequestType.WorkerShutdown;

/**
 * JAWNS: a Job scheduler with Amazon Web services and Nothing Shared
 */
public class Jawns {

    final AmazonSQS SQS;
    /** Does the Object-Relational Mapping for the Job Table */
    final DynamoDBMapper DBM;
    final AmazonS3 S3;
    private final AmazonSimpleEmailService SES;
    private final AmazonDynamoDB DB;

    /** NB: need to use US_EAST_1 (Northern Virginia) as has SES */
    private final static Regions MY_REGION = Regions.US_EAST_1;
    private final static AWSCredentialsProvider CREDS = new ClasspathPropertiesFileCredentialsProvider("aws-credentials.properties");
    final static Logger LOG = Logger.getLogger("Jawns");
    final static Gson GSON = new Gson();

    private static final OptionSpec Help;
    private static final OptionSpec<String> Project;
    private static final OptionSpec<String> WorkerFile;
    private static final OptionSpec<String> Email;
    private static final OptionSpec<Command> Cmd;
    private static final OptionSpec<String> SshUsername;
    private static final OptionParser Parser;
    private static OptionSet Options;

    static {
        Parser = new OptionParser();
        Project = Parser.accepts("project", "project name")
                .withRequiredArg().ofType(String.class).required();
        WorkerFile = Parser.accepts("workers", "file containing worker hostnames, one per line")
                .withRequiredArg().ofType(String.class).required();
        Email = Parser.accepts("email", "send email to this address when jobs fail/succeed")
                .withRequiredArg().ofType(String.class);
        SshUsername = Parser.accepts("ssh-user", "ssh to worker nodes using this username")
                .withRequiredArg().ofType(String.class);
        Cmd = Parser.accepts("command", "command to run, one of "+Arrays.toString(Command.values()))
                .withRequiredArg().ofType(Command.class).required();
        Help = Parser.accepts("help", "Print this help message").forHelp();
    }

    /**
     * The names of the AWS data structures we use
     */
    final String JOB_TABLE, S3_BUCKET;
    String RESULTQ_URL, COMMANDQ_URL;

    private final static String JT_JOBID = "jobId";
    private final static String JT_STATUS = "jobStatus";

    /**
     * List of the hostnames for each worker
     */
    private List<String> workerHostnames;

    /**
     * Creates the required AWS SQS/SimpleDB data structures used by JAWNS. This is idempotent.
     *
     * @param projectName a unique tag for this set of jobs and workers, to allow for concurrent JAWNS instances
     */
    public Jawns(String projectName) {

        JOB_TABLE = "jawns_" + projectName + "_jobtable";
        S3_BUCKET = "jawns." + projectName + ".bucket";
        final String RESULTQ = "jawns_" + projectName + "_resultq.fifo";
        final String COMMANDQ = "jawns_" + projectName + "_commandq.fifo";

        SQS = AmazonSQSClientBuilder.standard()
                .withRegion(MY_REGION)
                .withCredentials(CREDS)
                .build();
        SES = AmazonSimpleEmailServiceClientBuilder.standard()
                .withRegion(MY_REGION)
                .withCredentials(CREDS)
                .build();
        S3 = AmazonS3ClientBuilder.standard()
                .withRegion(MY_REGION)
                .withCredentials(CREDS)
                .build();
        DB = AmazonDynamoDBClientBuilder.standard()
                .withRegion(MY_REGION)
                .withCredentials(CREDS)
                .build();
        /* Configuration for DynamoDBMapper ORM mechanism. Always use consistent reads, always completely overwrite
         * existing object on save, use table name for this Jawns project */
        DynamoDBMapperConfig dfConf = DynamoDBMapperConfig.builder()
                .withConsistentReads(DynamoDBMapperConfig.ConsistentReads.CONSISTENT)
                .withSaveBehavior(DynamoDBMapperConfig.SaveBehavior.CLOBBER)
                .withTableNameOverride(DynamoDBMapperConfig.TableNameOverride.withTableNameReplacement(JOB_TABLE))
                .build();
        DBM = new DynamoDBMapper(DB,dfConf);

        // log all messages to disk
        System.err.format("%d log handlers by default %n",LOG.getHandlers().length);

        LOG.setLevel(Level.ALL);
        try {
            ConsoleHandler ch = new ConsoleHandler();
            ch.setFormatter(new SimpleFormatter());
            ch.setLevel(Level.ALL);
            LOG.addHandler(ch);
            FileHandler fh = new FileHandler("jawns.log", true);
            fh.setFormatter(new SimpleFormatter());
            fh.setLevel(Level.ALL);
            LOG.addHandler(fh);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }

        // setup AWS data structures
        try {
            CreateTableRequest table = DBM.generateCreateTableRequest(JobTableEntry.class);
            table.withBillingMode(BillingMode.PAY_PER_REQUEST);
            // Create table if it does not exist yet
            TableUtils.createTableIfNotExists(DB, table);
            TableUtils.waitUntilActive(DB, JOB_TABLE);

            Map<String, String> qAttrs = new HashMap<>();
            qAttrs.put("FifoQueue", "true");
            qAttrs.put("ContentBasedDeduplication", "true");
            qAttrs.put("ReceiveMessageWaitTimeSeconds", "20"); // long-polling, 20 is the max
            ListQueuesResult lqr = SQS.listQueues();
            if (lqr.getQueueUrls().stream().noneMatch(q -> q.contains(RESULTQ))) {
                CreateQueueRequest cqr = new CreateQueueRequest(RESULTQ).withAttributes(qAttrs);
                CreateQueueResult result = SQS.createQueue(cqr);
                RESULTQ_URL = result.getQueueUrl();
                LOG.info("created " + RESULTQ_URL);
            } else {
                RESULTQ_URL = lqr.getQueueUrls().stream().filter(q -> q.contains(RESULTQ)).findFirst().get();
                LOG.info("connected to existing SQS " + RESULTQ_URL);
            }
            if (lqr.getQueueUrls().stream().noneMatch(q -> q.contains(COMMANDQ))) {
                CreateQueueRequest cqr = new CreateQueueRequest(COMMANDQ).withAttributes(qAttrs);
                CreateQueueResult result = SQS.createQueue(cqr);
                COMMANDQ_URL = result.getQueueUrl();
                LOG.info("created " + COMMANDQ_URL);
            } else {
                COMMANDQ_URL = lqr.getQueueUrls().stream().filter(q -> q.contains(COMMANDQ)).findFirst().get();
                LOG.info("connected to existing SQS " + COMMANDQ_URL);
            }

            if (S3.listBuckets().stream().noneMatch(b -> b.getName().equals(S3_BUCKET))) {
                S3.createBucket(S3_BUCKET);
            }

            //System.err.println(DateTimeFormatter.ISO_INSTANT.format(Instant.now()));

        } catch (InterruptedException | AmazonClientException e) {
            LOG.severe(t2s(e, "error setting up AWS data structures"));
            System.exit(1);
        }
    }

    /**
     * submit the given list of Jobs to the worker nodes
     */
    public boolean submitJobs(List<Job> theJobs) {

        // find max jobid in JOB_TABLE
        List<JobTableEntry> jobs = DBM.scan(JobTableEntry.class, new DynamoDBScanExpression().withProjectionExpression(JT_JOBID));
        int jid = jobs.stream().mapToInt(JobTableEntry::getJobId).max().orElse(1);
        LOG.info("submitJobs() using initial jobid of "+jid);

        // submit jobs
        for (Job submitJob : theJobs) {

            assert !(submitJob instanceof JobTableEntry);

            // finalize the values for various fields
            JobTableEntry jte = new JobTableEntry(submitJob)
                    .withJobId(jid)
                    .withJobStatus(Available);

            // upload pre-files to S3
            int fileNum = 0;
            boolean ulPreFilesOk = true;
            for (Map.Entry<LocalFile,RemoteFile> sw : submitJob.preFiles().entrySet()) {
                File local = sw.getKey();
                if (!local.canRead()) {
                    LOG.severe("Can't read pre-file "+local+" so skipping job "+jte.getJobId());
                    ulPreFilesOk = false;
                    break;
                }
                String key = String.format("job%d.pre%d", jte.getJobId(), fileNum);
                try {
                    S3.putObject(S3_BUCKET, key, local);
                    jte.getS3PreFiles().put(key, sw.getValue());
                    fileNum++;
                } catch (AmazonClientException e) {
                    LOG.severe(t2s(e, "error uploading pre file "+local));
                    ulPreFilesOk = false;
                    break;
                }
            }

            if (!ulPreFilesOk) continue; // skip this Job

            try {
                DBM.save(jte);
                LOG.info("submitted job "+jte.getJobId());
            } catch (AmazonClientException e) {
                LOG.severe(t2s(e, "error submitting jobs"));
                return false;
            }
            jid++;
        }

        return true;
    }

    /**
     * Run a JAWNS command
     */
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

        Jawns J = new Jawns(Options.valueOf(Project));

        try {
            List<String> workers = Files.readAllLines(Paths.get(Options.valueOf(WorkerFile)), StandardCharsets.UTF_8);
            J.workerHostnames = workers.stream().map(String::trim).filter(w -> !w.equals("")).collect(Collectors.toList());
        } catch (IOException e) {
            LOG.severe(t2s(e, "error reading worker file: " + Options.valueOf(WorkerFile)));
            System.exit(1);
        }

        // process command
        final Command cmd = Options.valueOf(Cmd);
        switch (cmd) {
            case startup:
                J.startup();
                break;
            case gather:
                J.gatherResults();
                break;
            case shutdown:
                J.shutdown();
                break;
            case jobstatus:
                J.jobStatus();
                break;
            case canceljob: {
                System.out.print("Job ID to cancel: ");
                System.out.flush();
                Scanner scanner = new Scanner(System.in);
                int jobidToCan = Integer.valueOf(scanner.next().trim());
                J.cancelJob(jobidToCan);
                break; }
            case cancelalljobs:
                J.cancelAllJobs();
                break;
            case nuke: {
                System.out.print("Are you sure you want to E(mpty) or D(elete) ALL Jawns data structures? (E/D/n) ");
                System.out.flush();
                Scanner scanner = new Scanner(System.in);
                String answer = scanner.next().trim();
                switch (answer) {
                    case "E":
                        J.nuke(false);
                        System.out.println("Jawns data structures emptied");
                        break;
                    case "D":
                        J.nuke(true);
                        System.out.println("Jawns data structures deleted");
                        break;
                    default:
                        System.out.println("nuke operation cancelled");
                }
                break; }
            default:
                assert false : cmd;
                break;
        }
    }

    /** Get a worker daemon up and running on each worker */
    private void startup() {
        for (String wh : workerHostnames) {
            if (Options.has(SshUsername)) {
                wh = Options.valueOf(SshUsername)+"@"+wh;
            }
            try {
                ProcessBuilder pb = new ProcessBuilder("scp","target/jawns-1.0-SNAPSHOT-jar-with-dependencies.jar",wh+":jawns/");
                check_call(pb);
                String javaCmd = "'cd jawns && nohup java -Xmx500m -ea -cp jawns-1.0-SNAPSHOT-jar-with-dependencies.jar jawns.Worker --project "+Options.valueOf(Project)+"'";
                pb = new ProcessBuilder("/bin/bash","-c","ssh "+wh+" "+javaCmd);
                Process proc = pb.start();
                boolean exited = proc.waitFor(1, TimeUnit.SECONDS);
                if (exited) {
                    String stdout = IOUtils.toString(proc.getErrorStream(),Charset.defaultCharset());
                    String stderr = IOUtils.toString(proc.getErrorStream(),Charset.defaultCharset());
                    LOG.severe("problem launching worker on "+wh+System.lineSeparator()+
                            "stdout:"+stdout+System.lineSeparator()+"stderr:"+stderr);
                } else {
                    LOG.info("launched worker on "+wh);
                }
            } catch (InterruptedException | IOException e) {
                e.printStackTrace();
            }
        }
    }

    /** Listen on results queue for incoming results, and write them to disk. */
    private void gatherResults() {

        while (true) {
            sleep(1, TimeUnit.SECONDS);

            ReceiveMessageRequest recv = new ReceiveMessageRequest()
                    .withQueueUrl(RESULTQ_URL)
                    .withMaxNumberOfMessages(10)
                    .withAttributeNames(QueueAttributeName.All);
            ReceiveMessageResult res;
            try {
                res = SQS.receiveMessage(recv);
            } catch (AmazonClientException e) {
                LOG.severe(t2s(e,"error receiving RESULTQ messages"));
                continue;
            }

            for (Message msg : res.getMessages()) {
                WorkerResult wr = GSON.fromJson(msg.getBody(), WorkerResult.class);
                LOG.info("Received WorkerResult: "+msg.getBody());
                final String messageReceiptHandle = msg.getReceiptHandle();
                try {
                    SQS.deleteMessage(new DeleteMessageRequest(RESULTQ_URL, messageReceiptHandle));
                } catch (AmazonClientException e) {
                    LOG.severe(t2s(e,"error deleting RESULTQ message"));
                }

                String jName = wr.jobName();
                switch (wr.type) {
                    case JobFailed: {
                        // send email
                        sendEmail(jName+" job failed",jName+" failed with exit code "+wr.exitCode+ (wr.jobTimedOut ? " (hit timeout)" : ""));

                        // clear out pre-files from S3
                        try {
                            JobTableEntry theJob = DBM.load(JobTableEntry.class, Failed, wr.jobid);
                            assert null != theJob : wr.jobid;
                            clearS3Files(theJob);
                        }  catch (AmazonClientException e) {
                            LOG.severe(t2s(e, "error clearing S3 files for job"+wr.jobid));
                        }
                        LOG.info(jName+" failed with exit code "+wr.exitCode);
                        break; }
                    case JobSucceeded: {
                        File jobdir = jobdir(wr);
                        jobdir.mkdirs();

                        // send email
                        sendEmail(jName+" job succeeded","");

                        // download post-files from S3 (and then clear S3 files for this job)
                        try {
                            JobTableEntry theJob = DBM.load(JobTableEntry.class, Succeeded, wr.jobid);
                            assert null != theJob;
                            boolean dlAllPostFiles = theJob.getS3PostFiles().entrySet().stream().allMatch(e -> {
                                // relative paths are interpreted wrt jobdir
                                File local = e.getValue().isAbsolute() ? e.getValue() : new File(jobdir, e.getValue().getPath());
                                return downloadS3File(e.getKey(), local);
                            });
                            clearS3Files(theJob);
                            LOG.info(jName+" succeeded with exit code "+wr.exitCode);
                            if (!dlAllPostFiles) {
                                LOG.severe("error downloading post-files for job"+wr.jobid);
                            }
                        }  catch (AmazonClientException e) {
                            LOG.severe(t2s(e, "error downloading post-files for job"+wr.jobid));
                        }
                        break; }
                    case JobOutput:
                        File jobdir = jobdir(wr);
                        jobdir.mkdirs();
                        final File f;
                        switch (wr.outputType) {
                            case JobStdout:
                                f = new File(jobdir, wr.jobName()+".stdout");
                                assert f.length() == wr.startingByteIndex : "mismatched length,SBI "+f.length()+","+wr.startingByteIndex;
                                break;
                            case JobStderr:
                                f = new File(jobdir, wr.jobName()+".stderr");
                                assert f.length() == wr.startingByteIndex : "mismatched length,SBI "+f.length()+","+wr.startingByteIndex;
                                break;
                            case JobLog:
                                f = new File(jobdir, wr.jobName()+".log");
                                break;
                            default:
                                LOG.severe("Unknown WorkerResult.outputType " + wr.outputType);
                                continue; // goto next message
                        }
                        try (FileOutputStream fos = new FileOutputStream(f,true)) {
                            fos.write(wr.output);
                        } catch (IOException e) {
                            LOG.severe(t2s(e,"error writing to stdout/stderr/log file"));
                        }
                        break;
                }

            }
        }
    }

    /** Shut down each worker daemon via the command queue. Each worker exits after its current job completes */
    private void shutdown() {
        for (String wh : workerHostnames) {
            WorkerRequest wr = new WorkerRequest(wh, WorkerShutdown);

            SendMessageRequest send = new SendMessageRequest()
                    .withQueueUrl(COMMANDQ_URL)
                    .withMessageGroupId(Worker.SQS_MSG_GROUP_ID)
                    .withMessageBody(GSON.toJson(wr));
            try {
                SQS.sendMessage(send);
                LOG.info("told "+wh+" to shutdown");
            } catch (AmazonClientException e) {
                LOG.severe(t2s(e, "error writing WorkerRequest "+wr.type+" to command queue for "+wh));
            }
        }
    }

    /** Print out the current state of the Job Table */
    private void jobStatus() {
        System.out.println(" jobid             status tag");
        System.out.println(" -----             ------ ---");

        List<JobTableEntry> jobs = DBM.scan(JobTableEntry.class, new DynamoDBScanExpression());

        for (JobTableEntry j : jobs) {
            System.out.format("%,6d %18s %s %n", j.getJobId(), j.getJobStatus(), j.getTag());
        }
    }

    /** change status of job j in JT to CancelRequested. If a worker is running that job,
     * it will cancel it and grab the next job. Worker sets state of j in JT to Cancelled.
     */
    private void cancelJob(int jobid) {
        JobTableEntry jobToCancel = DBM.load(JobTableEntry.class, Available, jobid);
        JobStatus newStatus = Canceled;
        if (null == jobToCancel) {
            jobToCancel = DBM.load(JobTableEntry.class, Running, jobid);
            newStatus = CancelRequested;
            if (null == jobToCancel) {
                System.out.format("Cannot cancel job %d, it has already completed or been canceled", jobid);
                return;
            }
        }
        boolean changed = changeJobStatus(jobToCancel, newStatus);
        System.out.println(changed ? (Canceled == newStatus ? "job cancelled" : "job running, cancellation requested") : "job cancellation failed");
    }

    /** Cancel all jobs */
    private void cancelAllJobs() {

        try { // delete all Available/Succeeded/Failed jobs
            List<JobTableEntry> theJobs = DBM.query(JobTableEntry.class, statusQuery(Available));
            theJobs.forEach(DBM::delete);
            theJobs = DBM.query(JobTableEntry.class, statusQuery(Succeeded));
            theJobs.forEach(DBM::delete);
            theJobs = DBM.query(JobTableEntry.class, statusQuery(Failed));
            theJobs.forEach(DBM::delete);
        } catch (AmazonClientException e) {
            LOG.severe(t2s(e,"error deleting Available jobs"));
        }

        { // for each Running job, atomically set it to CancelRequested
            List<JobTableEntry> runningJobs = DBM.query(JobTableEntry.class, statusQuery(Running));

            for (JobTableEntry rj : runningJobs) {
                boolean changed = changeJobStatus(rj,CancelRequested);
                if (!changed) {
                    LOG.warning("problem cancelling job "+rj.getJobId());
                }
                // go on to the next job regardless
            }
        }

        // wait for workers to cancel their jobs
        sleep(1, TimeUnit.SECONDS);

        try { // delete all Canceled jobs
            List<JobTableEntry> availableJobs = DBM.query(JobTableEntry.class, statusQuery(Canceled));
            availableJobs.forEach(DBM::delete);
        } catch (AmazonClientException e) {
            LOG.severe(t2s(e,"error deleting Canceled jobs"));
        }
    }

    /** empty all the Jawns AWS data structures
     * @param hard if true, delete data structures instead of just emptying them */
    private void nuke(boolean hard) {
        // delete job table 1) to support schema changes and 2) because re-creating the JT isn't so bad
        try {
            // Job Table
            if (DB.listTables().getTableNames().stream().anyMatch(t -> t.equals(JOB_TABLE))) {
                if (hard) {
                    DB.deleteTable(JOB_TABLE);
                    LOG.info("deleted job table");
                } else {
                    List<JobTableEntry> allJobs = DBM.scan(JobTableEntry.class, new DynamoDBScanExpression());
                    for (JobTableEntry j : allJobs) {
                        DBM.delete(j);
                    }
                    LOG.info("emptied job table");
                }
            }

            // Result Queue, Command Queue
            if (SQS.listQueues().getQueueUrls().stream().anyMatch(u -> u.equals(RESULTQ_URL))) {
                if (hard) {
                    SQS.deleteQueue(RESULTQ_URL);
                    LOG.info("deleted result queue");
                } else {
                    SQS.purgeQueue(new PurgeQueueRequest(RESULTQ_URL));
                    LOG.info("emptied result queue");
                }
            }
            if (SQS.listQueues().getQueueUrls().stream().anyMatch(u -> u.equals(COMMANDQ_URL))) {
                if (hard) {
                    SQS.deleteQueue(COMMANDQ_URL);
                    LOG.info("deleted command queue");
                } else {
                    SQS.purgeQueue(new PurgeQueueRequest(COMMANDQ_URL));
                    LOG.info("emptied command queue");
                }
            }

            // empty S3 bucket
            if (S3.listBuckets().stream().anyMatch(b -> b.getName().equals(S3_BUCKET))) {

                ObjectListing objectListing = S3.listObjects(S3_BUCKET);
                while (true) {
                    for (S3ObjectSummary s3ObjectSummary : objectListing.getObjectSummaries()) {
                        S3.deleteObject(S3_BUCKET, s3ObjectSummary.getKey());
                    }

                    // If the bucket contains many objects, the listObjects() call
                    // might not return all of the objects in the first listing. Check to
                    // see whether the listing was truncated. If so, retrieve the next page of objects
                    // and delete them.
                    if (objectListing.isTruncated()) {
                        objectListing = S3.listNextBatchOfObjects(objectListing);
                    } else {
                        break;
                    }
                }
                LOG.info("emptied S3 bucket");

                // NB: don't delete the S3 bucket, as it takes *hours* to be able to re-create it
//                if (hard) {
//                    S3.deleteBucket(S3_BUCKET);
//                    LOG.info("deleted S3 bucket");
//                }
            }

        } catch (AmazonClientException e) {
            LOG.severe(t2s(e,"error with nuke"));
        }
    }

    /** changes the status of JobTableEntry j in the JobTableEntry Table, using Delete+Put in a transaction.
     * NB: the object j's status is not changed
     * @return true if the status was changed successfully, false otherwise */
    boolean changeJobStatus(JobTableEntry j, JobStatus newStatus) {
        HashMap<String, AttributeValue> existingKeys = new HashMap<>();
        existingKeys.put(JT_STATUS, new AttributeValue().withS(j.getJobStatus().name()));
        existingKeys.put(JT_JOBID, new AttributeValue().withN(String.valueOf(j.getJobId())));

        // delete existing job
        Delete delExisting = new Delete()
                .withTableName(JOB_TABLE)
                .withKey(existingKeys)
                .withConditionExpression("attribute_exists("+JT_STATUS+")"); // delete iff job exists

        // put new job
        Map<String, AttributeValue> newAttrs = DBM.getTableModel(JobTableEntry.class).convert(j);
        newAttrs.put(JT_STATUS, new AttributeValue(newStatus.name()));
        Put putNew = new Put()
                .withTableName(JOB_TABLE)
                .withItem(newAttrs)
                .withConditionExpression("attribute_not_exists("+JT_STATUS+")"); // put iff job doesn't already exist

        // run transaction
        TransactWriteItemsRequest switchStatus = new TransactWriteItemsRequest()
                .withTransactItems(Arrays.asList(
                        new TransactWriteItem().withDelete(delExisting),
                        new TransactWriteItem().withPut(putNew)));
        try {
            DB.transactWriteItems(switchStatus);
            return true;
        } catch (TransactionCanceledException e) {
            List<String> reasons = e.getCancellationReasons().stream().map(r -> r.getMessage()).collect(Collectors.toList());
            LOG.severe(t2s(e,reasons.toString()));
        } catch (AmazonClientException e) {
            LOG.severe(t2s(e,"error switching job status"));
        }
        return false;
    }

    /** Used to query for all jobs with the given JobStatus */
    DynamoDBQueryExpression<JobTableEntry> statusQuery(JobStatus jobStatus) {
        Map<String, AttributeValue> eav = new HashMap<>();
        eav.put(":val", new AttributeValue().withS(jobStatus.name()));

        return new DynamoDBQueryExpression<JobTableEntry>()
                .withKeyConditionExpression(JT_STATUS + " = :val")
                .withExpressionAttributeValues(eav);
    }

    /** @return true if file downloaded successfully, false otherwise */
    boolean downloadS3File(String s3id, File localFile) {
        try {
            LOG.info("Downloading "+s3id+" => "+localFile);
            S3Object s3o = S3.getObject(S3_BUCKET, s3id);
            S3ObjectInputStream s3is = s3o.getObjectContent();
            FileOutputStream fos = new FileOutputStream(localFile);
            byte[] read_buf = new byte[1024];
            int read_len;
            while ((read_len = s3is.read(read_buf)) > 0) {
                fos.write(read_buf, 0, read_len);
            }
            s3is.close();
            fos.close();
            return true;
        } catch (IOException | AmazonClientException e) {
            LOG.severe(t2s(e, "error downloading file "+localFile));
        }
        return false;
    }

    /** Remove pre/post files uploaded to S3 for this job */
    private void clearS3Files(JobTableEntry j) {
        if (null != j.getS3PreFiles()) {
            j.getS3PreFiles().forEach((s3id, unused) -> {
                try {
                    S3.deleteObject(S3_BUCKET, s3id);
                } catch (AmazonClientException e) {
                    LOG.severe(t2s(e, "error deleting S3 object " + s3id));
                }
            });
        }
        if (null != j.getS3PostFiles()) {
            j.getS3PostFiles().forEach((s3id, unused) -> {
                try {
                    S3.deleteObject(S3_BUCKET, s3id);
                } catch (AmazonClientException e) {
                    LOG.severe(t2s(e, "error deleting S3 object " + s3id));
                }
            });
        }
    }

    private void sendEmail(String subject, String body) {
        if (!Options.has(Email)) return;

        try {
            SendEmailRequest request = new SendEmailRequest()
                    .withDestination(new Destination().withToAddresses(Options.valueOf(Email)))
                    .withMessage(new com.amazonaws.services.simpleemail.model.Message()
                            .withBody(new Body()
                                    .withText(new Content().withCharset("UTF-8").withData(body)))
                            .withSubject(new Content().withCharset("UTF-8").withData(subject)))
                    .withSource("jawns.noreply@gmail.com");
            SES.sendEmail(request);
        } catch (AmazonClientException e) {
            LOG.severe(t2s(e, "error sending email"));
        }
    }

    private File jobdir(WorkerResult wr) {
        return new File(wr.jobName());
    }

    /**
     * Helper function to convert a Throwable's stack trace to a string
     */
    static String t2s(Throwable t, String msg) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        t.printStackTrace(pw);
        pw.println();
        pw.println(t.getLocalizedMessage());
        pw.println(msg);
        return sw.toString();
    }

    static void sleep(int duration, TimeUnit units) {
        long millis = units.toMillis(duration);
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {}
    }

    static void check_call(ProcessBuilder pb) throws IOException, InterruptedException {
        Process proc = pb.start();
        int rc = proc.waitFor();
        if (0 != rc) {
            throw new IOException("process " + pb.command().toString() + " exited with code " + rc);
        }
    }

    static ProcessOutput check_output(ProcessBuilder pb) throws IOException, InterruptedException {
        Process proc = pb.start();
        int rc = proc.waitFor();
        if (0 != rc) {
            throw new IOException("process " + pb.command().toString() + " exited with code " + rc);
        }
        return new ProcessOutput(
                IOUtils.toString(proc.getInputStream(), Charset.defaultCharset()),
                IOUtils.toString(proc.getErrorStream(), Charset.defaultCharset()));
    }

}

class ProcessOutput {
    final String stdout;
    final String stderr;

    ProcessOutput(String stdout, String stderr) {
        this.stdout = stdout;
        this.stderr = stderr;
    }
}

/** State transition diagram:
 * Available => Running, Canceled
 * Running => Succeeded, Failed, CancelRequested
 * CancelRequested => Canceled
 */
enum JobStatus { Available, Running, Succeeded, Failed, CancelRequested, Canceled }


enum WorkerResultType { JobSucceeded, JobFailed, JobOutput }
enum JobOutputType { JobStdout, JobStderr, JobLog }

/** A message from the worker to the results-gathering daemon */
class WorkerResult {
    // used for all WorkerResult messages
    WorkerResultType type = null;
    int jobid;
    private String jobTag;
    String workerHostname = null;

    /** used only for JobSucceeded/JobFailed */
    int exitCode;
    boolean jobTimedOut = false;

    // used only for JobOutput
    JobOutputType outputType = null;
    long startingByteIndex = -1;
    byte[] output = null;


    /** Initialize our job ID and tag fields from the given JobTableEntry */
    WorkerResult(JobTableEntry jte) {
        this.jobid = jte.getJobId();
        this.jobTag = jte.getTag();
    }

    /** @return a name containing this job's id and (if available) tag */
    String jobName() {
        return "job"+jobid+ (null == jobTag ? "" : "-"+jobTag);
    }
}

/** WorkerShutdown: tells worker to shutdown after the current job completes */
enum WorkerStatusRequestType { WorkerShutdown }

class WorkerRequest implements Serializable {
    final private int nonce;

    final WorkerStatusRequestType type;

    /**
     * @param something a string that is unique wrt other messages in the queue, so that the queue's
     *                  content-based deduplication doesn't drop the message
     * @param request what we want the worker to do */
    WorkerRequest(String something, WorkerStatusRequestType request) {
        nonce = something.hashCode();
        type = request;
    }
}

/* Message sent by a worker when we check its status
class WorkerStatusResponse {
    int jobid;
    String jobTag;
    String hostname;

    WorkerStatusResponse(JobTableEntry job, String hostname) {
        jobid = job.getJobId();
        jobTag = job.getTag();
        this.hostname = hostname;
    }
}
 */