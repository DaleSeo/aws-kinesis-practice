package seo.dale.practice.aws.kinesis.stream;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.ListStreamsResult;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.*;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

public class StreamManagerImplTest {
    private static AmazonKinesis kinesis;
    private static StreamManager manager;

    @BeforeClass
    public static void setUp() throws Exception {
        kinesis = AmazonKinesisClientBuilder.defaultClient();
        manager = new StreamManagerImpl(kinesis);
    }

    @Test
    public void mustCRUD() {
        String streamName = "Test-" + UUID.randomUUID().toString();
        int shardCount = RandomUtils.nextInt(4) + 1;

        // Create a stream
        manager.createStream(streamName, shardCount);

        // List streams
        List<String> streamNames = manager.listStreams();
        System.out.println(String.format("## streamNames: %s", streamNames));
        assertThat(streamNames).contains(streamName);

        // Get the stream
        StreamInfo streamInfo = manager.getStream(streamName);
        System.out.println(String.format("## streamInfo: %s", streamInfo));

        assertThat(streamInfo.getName()).isEqualTo(streamName);
        assertThat(streamInfo.getStatus()).isEqualTo("ACTIVE");
        assertThat(streamInfo.getArn()).startsWith("arn:aws:kinesis:").contains("Test-");

        // Delete the stream
        manager.deleteStream(streamName);

        try {
            manager.getStream(streamName);
            fail();
        } catch (RuntimeException e) {
            assertThat(e.getMessage()).startsWith("Failed to get the stream");
        }
    }

    @AfterClass
    public static void tearDown() throws Exception {
        ListStreamsResult result = kinesis.listStreams();
        result.getStreamNames()
                .stream()
                .filter(streamName -> streamName.startsWith("Test-"))
                .forEach(streamName -> {
                    System.out.println(String.format("deleting the stream, %s", streamName));
                    kinesis.deleteStream(streamName);
                });
    }
}