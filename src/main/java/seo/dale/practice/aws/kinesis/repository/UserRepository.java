package seo.dale.practice.aws.kinesis.repository;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import seo.dale.practice.aws.kinesis.model.User;

import java.util.Collection;
import java.util.Hashtable;
import java.util.Map;

public class UserRepository {
    private static Logger log = LoggerFactory.getLogger(UserRepository.class);

    private Map<String, User> userTable;

    public UserRepository() {
        this.userTable = new Hashtable<>();
    }

    public void insert(User user) {
        log.debug("inserting a user: {}", user);
        userTable.put(user.getId(), user);
    }

    public Collection<User> findAll() {
        return userTable.values();
    }
}
