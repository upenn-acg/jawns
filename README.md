## JAWNS: Job scheduling with Amazon Web services and Nothing Shared

Running a series of batch jobs on a set of machines is a frequent, but annoying, task. Running things manually scales ok up to a few jobs, but beyond that is very tedious and inefficient as it is hard to track progress. Setting up a proper cluster job management system like [Slurm](https://slurm.schedmd.com) or [Sun/Oracle Grid Engine](https://arc.liv.ac.uk/trac/SGE) is the "right answer" but takes a fair bit of work. Additionally, proper cluster job managers aren't as happy with a rapidly-changing set of worker nodes, e.g., if my cluster is built out of AWS EC2 instances or CloudLab nodes. I want something "simple" that I can bootstrap in one minute, with *no* installation dependencies.

The core idea of JAWNS is to use Amazon Web Services to manage the queue of jobs, and all communication to/from worker nodes. All communication takes place via AWS data structures, so communication is fast and reliable in the face of network outages, so the JAWNS programs themselves don't need to maintain much state.

The overall workflow looks like this:

1. Write a Java program that links against the Jawns JAR and calls `boolean submitJobs(List<Job> theJobs)` to submit jobs to the cluster. [See example code](https://github.com/upenn-acg/jawns/blob/master/src/main/java/SubmitTestJobs.java).
2. Launch worker daemon(s) on the worker node(s) via the `startup` command
3. Gather results from the worker daemon(s) via the `gather` command
4. There are a variety of other commands for checking job status, cancelling jobs, etc. Run Jawns with `--help` for more details.

There's no need for the job submission, `startup`, `gather` or other commands to be run from the same machine, since all state is stored in AWS, making each command essentially stateless. A notable exception is the `gather` command, which saves output to the local machine.

Requirements:
* the worker nodes need Java ≥1.8 installed (just the JRE, no JDK needed)
* the `startup` command needs passwordless-ssh access to each worker node, to launch the worker daemon
* the worker nodes and any other place you run Jawns commands need internet (i.e., AWS) access
