package jawns;

import com.amazonaws.services.dynamodbv2.datamodeling.*;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

/** The internal Job representation, i.e., an entry in the Job Table
 * NB: the tableName attribute is ignored, and set dynamically via DynamoDBMapperConfig in the Jawns ctor
 */
@DynamoDBTable(tableName = "ignored")
final public class JobTableEntry extends Job {

    /** public no-arg ctor needed for DyanmoDBMapper */
    public JobTableEntry() {}

    /** copy constructor, shallow-copying over values of all Job j's fields */
    public JobTableEntry(Job j) {
        this.command = j.command;
        this.cwd = j.cwd;
        this.preFiles = j.preFiles;
        this.postFiles = j.postFiles;
        this.tag = j.tag;
        this.timeoutSeconds = j.timeoutSeconds;
    }


    /** job status */
    @DynamoDBTypeConverted(converter = JobStatusConverter.class)
    @DynamoDBHashKey
    public JobStatus getJobStatus() {
        return jobStatus;
    }
    public void setJobStatus(JobStatus jobStatus) {
        this.jobStatus = jobStatus;
    }
    public JobTableEntry withJobStatus(JobStatus jobStatus) {
        setJobStatus(jobStatus);
        return this;
    }
    private JobStatus jobStatus = null;


    /** id for this job. Initialized by JAWNS, not by user code. */
    @DynamoDBRangeKey
    public int getJobId() {
        return jobId;
    }
    public void setJobId(int jobId) {
        this.jobId = jobId;
    }
    public JobTableEntry withJobId(int jobId) {
        setJobId(jobId);
        return this;
    }
    private int jobId = -1;


    /** map of <S3ObjectID,LocalPath> used by worker to download pre-files */
    @DynamoDBAttribute
    @DynamoDBTypeConverted(converter = S3FilesConverter.class)
    public Map<String, File> getS3PreFiles() {
        return s3PreFiles;
    }
    public void setS3PreFiles(Map<String, File> s3PreFiles) {
        this.s3PreFiles = s3PreFiles;
    }
    private Map<String,File> s3PreFiles = new HashMap<>();


    /** map of <S3ObjectID,LocalPath> used by gatherer to download post-files */
    @DynamoDBAttribute
    @DynamoDBTypeConverted(converter = S3FilesConverter.class)
    public Map<String, File> getS3PostFiles() { return s3PostFiles; }
    public void setS3PostFiles(Map<String, File> m) {
        this.s3PostFiles = m;
    }
    private Map<String,File> s3PostFiles = new HashMap<>();


    /** @return a name containing this job's id and (if available) tag */
    public String name() {
        return "job"+jobId+ (null == tag ? "" : "-"+tag);
    }
}