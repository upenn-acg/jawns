## JAWNS: Job scheduling with Amazon Web services and Nothing Shared

Running a series of batch jobs on a set of machines is a frequent, but annoying, task. Running things manually scales ok up to a few jobs, but beyond that it is hard to track progress. Setting up a proper cluster job management system like [Slurm](https://slurm.schedmd.com) or [Sun/Oracle Grid Engine](https://arc.liv.ac.uk/trac/SGE) takes a bit of work. Additionally, proper cluster job managers aren't happy with a time-varying set of worker nodes, e.g., if your cluster is built out of AWS EC2 instances or [CloudLab](https://cloudlab.us) nodes. Jawns is designed to bootstrap itself from scratch in under a minute.

The core idea of JAWNS is to use Amazon Web Services to manage the job queue, and all communication to/from worker nodes. All communication takes place via AWS data structures, making communication fast and reliable in the face of network outages. The Jawns programs themselves can then maintain as little state as possible.

### Requirements
* the worker nodes need Java ≥1.8 installed (just the JRE, no JDK needed)
* the `startup` command needs passwordless-ssh access to each worker node, to launch the worker daemon
* the worker nodes, and Jawns commands, need internet access

No shared filesystem (e.g., NFS), or direct communication between Jawns components (e.g., via sockets), is required.

### Building & Running

After clong the repo, fill in your AWS credentials (access key and secret key) in `src/main/resources/aws-credentials.properties` (see [the sample file](https://github.com/upenn-acg/jawns/blob/master/src/main/resources/aws-credentials.properties.sample) for the required format). These credentials need to grant full access to the AWS services that Jawns uses: [SQS, SES, S3 and DynamoDB](https://github.com/upenn-acg/jawns/wiki/JAWNS-Architecture).

Then build the JAR file via maven:
```
mvn package
```

Then create a list of worker node hostnames, one line per file, in `workers.txt`. Finally, you can run a Jawns command:
```
java -jar target/jawns-1.0-SNAPSHOT-jar-with-dependencies.jar --workers workers.txt --project test --command status
```

### Typical workflow

1. Write a Java program that links against the Jawns JAR and calls `boolean submitJobs(List<Job> theJobs)` to submit jobs to the cluster. [See example code](https://github.com/upenn-acg/jawns/blob/master/src/main/java/SubmitTestJobs.java).
2. Launch worker daemons on the worker nodes via the `startup` command
3. Gather results from the worker daemons via the `gather` command
4. There are a variety of other commands for checking job status, cancelling jobs, etc. Run Jawns with `--help` for more details.

There's no need for the job submission, `startup`, `gather` or other commands to be run from the same machine, since all state is stored in AWS, making each command essentially stateless. A notable exception is the `gather` command, which saves output to the local machine.

### Features

* send email on job completion/failure
* transfer files to/from workers before/after job completes
* a job can be killed after a timeout is hit
