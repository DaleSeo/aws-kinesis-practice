package seo.dale.practice.aws.kinesis.consumer;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import seo.dale.practice.aws.kinesis.model.User;
import seo.dale.practice.aws.kinesis.model.UserConverter;
import seo.dale.practice.aws.kinesis.repository.UserRepository;

public class UserRecordProcessor implements IRecordProcessor {
    private static Logger log = LoggerFactory.getLogger(UserRecordProcessor.class);

    private final UserRepository userRepository;
    private String shardId;

    public UserRecordProcessor(UserRepository userRepository) {
        this.userRepository = userRepository;
    }

    @Override
    public void initialize(InitializationInput initializationInput) {
        log.debug("initialize... shardId: {}, ExtendedSequenceNumber: {}, PendingCheckpointSequenceNumber: {}",
                initializationInput.getShardId(),
                initializationInput.getExtendedSequenceNumber(),
                initializationInput.getPendingCheckpointSequenceNumber()
        );
        shardId = initializationInput.getShardId();
    }

    @Override
    public void processRecords(ProcessRecordsInput processRecordsInput) {
        log.debug("processRecords... MillisBehindLatest: {}, RecordsSize: {}",
                processRecordsInput.getMillisBehindLatest(),
                processRecordsInput.getRecords().size()
        );
        processRecordsInput.getRecords()
                .stream()
                .map(record -> {
                    User user = UserConverter.fromBytes(record.getData().array());
                    long age = System.currentTimeMillis() - user.getCreated();
                    log.debug("sequenceNumber: {}, partitionKey: {}, age: {}ms", record.getSequenceNumber(), record.getPartitionKey(), age);
                    return user;
                })
                .forEach(user -> userRepository.insert(user));
        checkpoint(processRecordsInput.getCheckpointer());
    }

    @Override
    public void shutdown(ShutdownInput shutdownInput) {
        log.debug("shutdown... shutdownReason: {}, checkpointer: {}",
                shutdownInput.getShutdownReason(),
                shutdownInput.getCheckpointer()
        );

        if (shutdownInput.getShutdownReason() == ShutdownReason.TERMINATE) {
            checkpoint(shutdownInput.getCheckpointer());
        }
    }

    private void checkpoint(IRecordProcessorCheckpointer checkpointer) {
        log.debug("Checkpointing shard: {}", shardId);
        try {
            checkpointer.checkpoint();
        } catch (InvalidStateException | ShutdownException e) {
            e.printStackTrace();
        }
    }
}
