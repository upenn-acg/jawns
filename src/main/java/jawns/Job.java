package jawns;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBIgnore;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTypeConverted;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

/** The user-facing description for a job
 * NB: the tableName attribute is ignored, and set dynamically via DynamoDBMapperConfig in the Jawns ctor
 * The preferred way to setup a Job object is via the fluent interface, by chaining the with...() methods
 */
@DynamoDBTable(tableName = "ignored")
public class Job {

    @DynamoDBAttribute
    public String getTag() {
        return tag;
    }
    public void setTag(String tag) {
        if (tag.contains(" ")) {
            throw new IllegalStateException("job tags cannot contain spaces, as the tag is used in many file/directory names");
        }
        this.tag = tag;
    }
    /** A descriptive tag used for this job. Tags cannot contain spaces, as they are used in file/directory names */
    public Job withTag(String tag) {
        setTag(tag);
        return this;
    }
    protected String tag = null;


    @DynamoDBAttribute
    public String getCommand() {
        return command;
    }
    public void setCommand(String command) {
        this.command = command;
    }
    /** shell command to run on the worker node */
    public Job withCommand(String command) {
        setCommand(command);
        return this;
    }
    protected String command = null;


    @DynamoDBAttribute
    public String getCwd() {
        return cwd;
    }
    public void setCwd(String cwd) {
        this.cwd = cwd;
    }
    /** directory (on worker node) from which to run `command` */
    public Job withCwd(String cwd) {
        setCwd(cwd);
        return this;
    }
    protected String cwd = null;


    @DynamoDBAttribute
    public int getTimeoutSeconds() {
        return timeoutSeconds;
    }
    public void setTimeoutSeconds(int timeoutSeconds) {
        this.timeoutSeconds = timeoutSeconds;
    }
    /** timeout (in seconds) after which the job should be killed. <=0 means no timeout */
    public Job withTimeoutSeconds(int timeoutSeconds) {
        setTimeoutSeconds(timeoutSeconds);
        return this;
    }
    protected int timeoutSeconds = -1;


    @DynamoDBIgnore
    public Map<LocalFile, RemoteFile> preFiles() {
        return preFiles;
    }
    /** transfer file local to worker before the job starts. The file will be named remote on the worker node. If remote
     * is a relative path, it will be interpreted relative to the job's specified cwd */
    public Job withPreFile(LocalFile local, RemoteFile remote) {
        preFiles.put(local,remote);
        return this;
    }
    protected Map<LocalFile,RemoteFile> preFiles = new HashMap<>();


    @DynamoDBAttribute
    @DynamoDBTypeConverted(converter = PostFilesConverter.class)
    public Map<RemoteFile, File> getPostFiles() {
        return postFiles;
    }
    public void setPostFiles(Map<RemoteFile, File> postFiles) {
        this.postFiles = postFiles;
    }
    /** transfer file remote to the gatherer after successful job completion. File will be named local on the gatherer.
     * If local is a relative path, it will be interpreted relative to the job's results directory */
    public Job withPostFile(RemoteFile remote, File local) {
        postFiles.put(remote, local);
        return this;
    }
    protected Map<RemoteFile, File> postFiles = new HashMap<>();
}

