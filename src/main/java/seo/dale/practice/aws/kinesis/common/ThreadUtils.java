package seo.dale.practice.aws.kinesis.common;

import java.util.concurrent.TimeUnit;

public class ThreadUtils {
    public static void sleepInSeconds(long seconds) {
        try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(seconds));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
