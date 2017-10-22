package seo.dale.practice.aws.kinesis.stream;

import lombok.Builder;
import lombok.Data;

@Builder
@Data
public class StreamInfo {
    private String name;
    private String arn;
    private String status;
}
