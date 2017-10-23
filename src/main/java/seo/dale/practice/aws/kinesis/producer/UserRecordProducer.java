package seo.dale.practice.aws.kinesis.producer;

import seo.dale.practice.aws.kinesis.model.User;

public interface UserRecordProducer {
    void produce(User user);
}
