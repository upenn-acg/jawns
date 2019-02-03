package jawns;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTypeConverter;

import java.io.File;
import java.util.Map;

import static java.util.stream.Collectors.toMap;

public class PostFilesConverter implements DynamoDBTypeConverter<Map<String, String>, Map<RemoteFile, File>> {
    @Override
    public Map<String, String> convert(Map<RemoteFile, File> postFilesMap) {
        return postFilesMap.entrySet()
                .stream()
                .collect(toMap(k -> k.getKey().toString(),
                        k -> k.getValue().toString()));
    }

    @Override
    public Map<RemoteFile, File> unconvert(Map<String, String> postFilesMap) {
        return postFilesMap.entrySet()
                .stream()
                .collect(toMap(k -> new RemoteFile(k.getKey()),
                        k -> new File(k.getValue())));
    }
}
