package seo.dale.practice.aws.kinesis.stream;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import org.junit.Before;
import org.junit.Test;

public class KclWorkerTest {
    private static final String APP_NAME = "TestStreamApp"; // used as the DynamoDB table name
    private static final String STREAM_NAME = "TestStream";
    private static final String REGION_NAME = "us-west-2";
    private static final String WORKER_ID = "TestWorker";

    private Worker worker;

    @Before
    public void setUp() throws Exception {
        AWSCredentialsProvider credentialsProvider = new ProfileCredentialsProvider();

        KinesisClientLibConfiguration kclConfiguration = new KinesisClientLibConfiguration(
                APP_NAME,
                STREAM_NAME,
                credentialsProvider,
                WORKER_ID
        )
                .withRegionName(REGION_NAME);

        IRecordProcessorFactory recordProcessorFactory = () -> new IRecordProcessor() {
            @Override
            public void initialize(InitializationInput initializationInput) {
                System.out.println("## processor initialize");
            }

            @Override
            public void processRecords(ProcessRecordsInput processRecordsInput) {
                System.out.println("## processor processRecords");
            }

            @Override
            public void shutdown(ShutdownInput shutdownInput) {
                System.out.println("## processor shutdown");
            }
        };

        worker = new Worker.Builder()
                .recordProcessorFactory(recordProcessorFactory)
                .config(kclConfiguration)
                .build();
    }

    @Test
    public void test() throws Exception {
        worker.run();
    }
}
