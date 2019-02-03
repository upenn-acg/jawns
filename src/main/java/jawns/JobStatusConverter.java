package jawns;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTypeConverter;

/** Object-relational mapping for JobStatus */
public class JobStatusConverter implements DynamoDBTypeConverter<String, JobStatus> {

    @Override
    public String convert(JobStatus status) {
        if (null == status) {
            return "";
        }
        return status.name();
    }

    @Override
    public JobStatus unconvert(String s) {
        if (null == s || s.equals("")) {
            return null;
        }
        return JobStatus.valueOf(s);
    }
}
