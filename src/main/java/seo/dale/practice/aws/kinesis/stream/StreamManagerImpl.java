package seo.dale.practice.aws.kinesis.stream;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.ListStreamsResult;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.model.StreamDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import seo.dale.practice.aws.kinesis.common.ThreadUtils;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class StreamManagerImpl implements StreamManager {
    private static final Logger log = LoggerFactory.getLogger(StreamManagerImpl.class);

    private AmazonKinesis kinesis;

    public StreamManagerImpl(AmazonKinesis kinesis) {
        this.kinesis = kinesis;
    }

    @Override
    public List<String> listStreams() {
        ListStreamsResult result = kinesis.listStreams();
        return result.getStreamNames();
    }

    @Override
    public StreamInfo getStream(String streamName) {
        StreamDescription description = getStreamDescription(streamName);
        return StreamInfo.builder()
                .name(description.getStreamName())
                .status(description.getStreamStatus())
                .arn(description.getStreamARN())
                .build();
    }

    private StreamDescription getStreamDescription(String streamName) {
        try {
            DescribeStreamResult result = kinesis.describeStream(streamName);
            return result.getStreamDescription();
        } catch (ResourceNotFoundException e) {
            throw new RuntimeException(String.format("Failed to get the stream: %s", streamName), e);
        }
    }

    @Override
    public void createStream(String streamName, Integer shardCount) {
        kinesis.createStream(streamName, shardCount);
        waitForStreamToBeCreated(streamName);
    }

    private void waitForStreamToBeCreated(String streamName) {
        long endTime = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(10);
        while (System.currentTimeMillis() < endTime) {
            ThreadUtils.sleepInSeconds(10);

            StreamDescription description = getStreamDescription(streamName);
            if ("ACTIVE".equalsIgnoreCase(description.getStreamStatus())) {
                log.info("Successfully created the stream: {}", description);
                return;
            }
            log.debug("Still creating the stream: {}", description);
        }

        throw new RuntimeException(String.format("Failed to create the stream: %s", streamName));
    }

    @Override
    public void deleteStream(String streamName) {
        kinesis.deleteStream(streamName);
        waitForStreamToBeDeleted(streamName);
    }

    private void waitForStreamToBeDeleted(String streamName) {
        long endTime = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(10);
        while (System.currentTimeMillis() < endTime) {
            ThreadUtils.sleepInSeconds(10);

            try {
                StreamDescription description = getStreamDescription(streamName);
                log.debug("Still deleting the stream: {}", description);
            } catch (RuntimeException e) {
                log.info("Successfully deleted the stream: {}", streamName);
                return;
            }
        }

        throw new RuntimeException(String.format("Failed to delete the stream: %s", streamName));
    }
}
