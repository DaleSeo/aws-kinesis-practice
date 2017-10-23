package seo.dale.practice.aws.kinesis.consumer;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import seo.dale.practice.aws.kinesis.repository.UserRepository;

public class UserStreamConsumer {
    private final Worker worker;

    public UserStreamConsumer(Worker worker) {
        this.worker = worker;
    }

    public void consume() {
        worker.run();
    }

    private static final String APPLICATION_NAME = "KinesisTest";
    private static final String STREAM_NAME = "Test";
    private static final String WORKER_NAME = "TestWorker";
    private static final String REGION_NAME = Regions.US_WEST_2.getName();

    public static void main(String[] args) {
        KinesisClientLibConfiguration kclConfig = new KinesisClientLibConfiguration(
                APPLICATION_NAME,
                STREAM_NAME,
                new ProfileCredentialsProvider(),
                WORKER_NAME
        ).withRegionName(REGION_NAME);

        UserRepository userRepository = new UserRepository();
        IRecordProcessorFactory recordProcessorFactory = () -> new UserRecordProcessor(userRepository);

        Worker worker = new Worker.Builder()
                .config(kclConfig)
                .recordProcessorFactory(recordProcessorFactory)
                .build();

        UserStreamConsumer streamConsumer = new UserStreamConsumer(worker);
        streamConsumer.consume();
    }
}
