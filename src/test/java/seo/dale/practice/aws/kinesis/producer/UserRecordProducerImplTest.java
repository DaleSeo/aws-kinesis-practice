package seo.dale.practice.aws.kinesis.producer;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import org.junit.Before;
import org.junit.Test;
import seo.dale.practice.aws.kinesis.model.User;

import java.util.UUID;

public class UserRecordProducerImplTest {
    private static final String STREAM_NAME = "Test";

    private AmazonKinesis kinesis;
    private UserRecordProducer targetService;

    @Before
    public void setUp() throws Exception {
        kinesis = AmazonKinesisClientBuilder.defaultClient();
        targetService = new UserRecordProducerImpl(kinesis, STREAM_NAME);
    }

    @Test
    public void test() throws Exception {
        User user = User.builder()
                .id(UUID.randomUUID().toString())
                .name("Dale")
                .email("dale.seo@gmail.com")
                .build();
        targetService.produce(user);
    }
}