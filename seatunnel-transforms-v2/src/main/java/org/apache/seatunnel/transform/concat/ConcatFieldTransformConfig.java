package org.apache.seatunnel.transform.concat;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.List;
import java.util.Map;

/**
 * @Author: Feng
 * @Date: 2023/11/28 16:15
 */
public class ConcatFieldTransformConfig {

    public static final Option<List<Map>> FIELDS =
            Options.key("fields")
                    .listType(Map.class)
                    .noDefaultValue()
                    .withDescription("字段拼接配置");
}
