package org.apache.seatunnel.transform.cases;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.io.Serializable;
import java.util.List;

/**
 * @Author: Feng
 * @Date: 2023/11/28 15:04
 */
public class CaseFieldTransformConfig implements Serializable {

    public static final Option<List<String>> UPPER_FIELDS =
            Options.key("upper_fields")
                    .listType()
                    .noDefaultValue()
                    .withDescription("转大写的字段项");

    public static final Option<List<String>> LOWER_FIELDS =
            Options.key("lower_fields")
                    .listType()
                    .noDefaultValue()
                    .withDescription("转小写的字段项");

}
