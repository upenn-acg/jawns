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
 */
@DynamoDBTable(tableName = "ignored")
public class Job {

    /** A descriptive tag used for this job. Tags cannot contain spaces. */
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
    public Job withTag(String tag) {
        setTag(tag);
        return this;
    }
    protected String tag = null;


    /** shell command to run on the worker node */
    @DynamoDBAttribute
    public String getCommand() {
        return command;
    }
    public void setCommand(String command) {
        this.command = command;
    }
    public Job withCommand(String command) {
        setCommand(command);
        return this;
    }
    protected String command = null;


    /** directory from which to run `command` */
    @DynamoDBAttribute
    public String getCwd() {
        return cwd;
    }
    public void setCwd(String cwd) {
        this.cwd = cwd;
    }
    public Job withCwd(String cwd) {
        setCwd(cwd);
        return this;
    }
    protected String cwd = null;


    /** timeout (in seconds) after which the job should be killed. <=0 means no timeout */
    @DynamoDBAttribute
    public int getTimeoutSeconds() {
        return timeoutSeconds;
    }
    public void setTimeoutSeconds(int timeoutSeconds) {
        this.timeoutSeconds = timeoutSeconds;
    }
    public Job withTimeoutSeconds(int timeoutSeconds) {
        setTimeoutSeconds(timeoutSeconds);
        return this;
    }
    protected int timeoutSeconds = -1;


    /** a map of <LocalFilename,RemoteFilename> to transfer to worker before the job starts */
    @DynamoDBIgnore
    public Map<LocalFile, RemoteFile> preFiles() {
        return preFiles;
    }
    public Job withPreFile(LocalFile local, RemoteFile remote) {
        preFiles.put(local,remote);
        return this;
    }
    protected Map<LocalFile,RemoteFile> preFiles = new HashMap<>();


    /** a map of <RemoteFilename,LocalFilename> to transfer to gatherer after successful job completion */
    @DynamoDBAttribute
    @DynamoDBTypeConverted(converter = PostFilesConverter.class)
    public Map<RemoteFile, File> getPostFiles() {
        return postFiles;
    }
    public void setPostFiles(Map<RemoteFile, File> postFiles) {
        this.postFiles = postFiles;
    }
    public Job withPostFile(RemoteFile remote, File local) {
        postFiles.put(remote, local);
        return this;
    }
    protected Map<RemoteFile, File> postFiles = new HashMap<>();
}

