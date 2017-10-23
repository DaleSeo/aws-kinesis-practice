package seo.dale.practice.aws.kinesis.producer;

import seo.dale.practice.aws.kinesis.model.User;

import java.util.UUID;

public class UserGenerator {
    public static User randomUser() {
        String id = UUID.randomUUID().toString();
        String name = "TestUser-" + id.substring(0, 8);
        String email = id.substring(0, 8) + "@test.com";
        return User.builder()
                .id(id)
                .name(name)
                .email(email)
                .created(System.currentTimeMillis())
                .build();
    }
}
