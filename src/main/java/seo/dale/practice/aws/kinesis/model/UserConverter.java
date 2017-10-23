package seo.dale.practice.aws.kinesis.model;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class UserConverter {
    private final static ObjectMapper JSON = new ObjectMapper();

    public static byte[] toBytes(User user) {
        try {
            return JSON.writeValueAsBytes(user);
        } catch (IOException e) {
            return null;
        }
    }

    public static User fromBytes(byte[] bytes) {
        try {
            return JSON.readValue(bytes, User.class);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
