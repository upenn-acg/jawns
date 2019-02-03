package jawns;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTypeConverter;

import java.io.File;
import java.util.Map;

import static java.util.stream.Collectors.toMap;

public class S3FilesConverter implements DynamoDBTypeConverter<Map<String, String>, Map<String, File>> {
    @Override
    public Map<String, String> convert(Map<String, File> s3FilesMap) {
        return s3FilesMap.entrySet()
                .stream()
                .collect(toMap(Map.Entry::getKey,
                        k -> k.getValue().getPath()));
    }

    @Override
    public Map<String, File> unconvert(Map<String, String> s3FilesMap) {
        return s3FilesMap.entrySet()
                .stream()
                .collect(toMap(Map.Entry::getKey,
                        k -> new File(k.getValue())));
    }
}
