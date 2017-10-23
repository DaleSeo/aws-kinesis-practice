package seo.dale.practice.aws.kinesis.producer;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import seo.dale.practice.aws.kinesis.model.User;
import seo.dale.practice.aws.kinesis.model.UserConverter;

import java.nio.ByteBuffer;

public class UserRecordProducerImpl implements UserRecordProducer {
    private static Logger log = LoggerFactory.getLogger(UserRecordProducerImpl.class);

    private final AmazonKinesis kinesis;
    private final String streamName;

    public UserRecordProducerImpl(AmazonKinesis kinesis, String streamName) {
        this.kinesis = kinesis;
        this.streamName = streamName;
    }

    @Override
    public void produce(User user) {
        log.info("Producing a user: {}", user);
        byte[] bytes = UserConverter.toBytes(user);
        PutRecordResult result = kinesis.putRecord(streamName, ByteBuffer.wrap(bytes), user.getId());
        log.debug("PutRecordResult: {}", result);
    }
}
