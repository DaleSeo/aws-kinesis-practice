package seo.dale.practice.aws.kinesis.producer;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import seo.dale.practice.aws.kinesis.common.ThreadUtils;
import seo.dale.practice.aws.kinesis.model.User;

public class UserStreamProducer {
    private UserRecordProducer recordProducer;

    public UserStreamProducer(UserRecordProducer recordProducer) {
        this.recordProducer = recordProducer;
    }

    public void produce(long interval) {
        while (true) {
            ThreadUtils.sleepInSeconds(interval);
            User user = UserGenerator.randomUser();
            recordProducer.produce(user);
        }
    }

    private static final String STREAM_NAME = "Test";

    public static void main(String[] args) {
        AmazonKinesis kinesis = AmazonKinesisClientBuilder.defaultClient();
        UserRecordProducer recodeProducer = new UserRecordProducerImpl(kinesis, STREAM_NAME);

        UserStreamProducer streamProducer = new UserStreamProducer(recodeProducer);
        streamProducer.produce(1);
    }
}
