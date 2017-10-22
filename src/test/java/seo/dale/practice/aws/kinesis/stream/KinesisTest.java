package seo.dale.practice.aws.kinesis.stream;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class KinesisTest {
    private AmazonKinesis kinesis;

    @Before
    public void setUp() throws Exception {
        kinesis = AmazonKinesisClientBuilder.defaultClient();
        assertThat(kinesis).isNotNull();
    }

    @Test
    public void test() throws Exception {
        kinesis.createStream("Test", 2);
        DescribeStreamResult result = kinesis.describeStream("Test");
        System.out.println(result);
    }
}
