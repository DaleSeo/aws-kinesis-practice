package seo.dale.practice.aws.kinesis.stream;

import java.util.List;

public interface StreamManager {
    List<String> listStreams();
    StreamInfo getStream(String streamName);
    void createStream(String streamName, Integer shardCount);
    void deleteStream(String streamName);
}
