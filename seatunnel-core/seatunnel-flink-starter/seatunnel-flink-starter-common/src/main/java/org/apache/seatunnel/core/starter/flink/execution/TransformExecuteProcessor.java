/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.core.starter.flink.execution;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;
import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.table.factory.TableTransformFactory;
import org.apache.seatunnel.api.table.factory.TableTransformFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.core.starter.exception.TaskExecuteException;
import org.apache.seatunnel.core.starter.execution.PluginUtil;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelTransformPluginDiscovery;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.translation.flink.serialization.FlinkRowConverter;
import org.apache.seatunnel.translation.flink.utils.TypeConverterUtils;

import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.apache.seatunnel.api.common.CommonOptions.RESULT_TABLE_NAME;

public class TransformExecuteProcessor
        extends FlinkAbstractPluginExecuteProcessor<TableTransformFactory> {

    protected TransformExecuteProcessor(
            Config pluginConfig,
            JobContext jobContext,
            Set<URL> jarPaths,
            Config envConfig) {
        super(pluginConfig, jobContext, jarPaths, envConfig);
    }

    @Override
    protected TableTransformFactory initializePlugin() {
        SeaTunnelTransformPluginDiscovery transformPluginDiscovery =
                new SeaTunnelTransformPluginDiscovery();
        return PluginUtil.createTransformFactory(transformPluginDiscovery, pluginConfig, jarPaths);
    }

    @Override
    public DataStreamTableInfo execute(List<DataStreamTableInfo> upstreamDataStreams)
            throws TaskExecuteException {
        Optional<DataStream<Row>> reduce = upstreamDataStreams.stream().map(t -> t.getDataStream())
                .reduce((t1, t2) -> t1.union(t2));
        if (!reduce.isPresent()) {
            throw new TaskExecuteException("该节点没有上游节点！");
        }
        DataStream<Row> stream = reduce.get();
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

        try {
            TableTransformFactory factory = plugin;
            TableTransformFactoryContext context =
                    new TableTransformFactoryContext(
                            Collections.singletonList(upstreamDataStreams.get(0).getCatalogTable()),
                            ReadonlyConfig.fromConfig(pluginConfig),
                            classLoader);
            ConfigValidator.of(context.getOptions()).validate(factory.optionRule());
            SeaTunnelTransform transform = factory.createTransform(context).createTransform();

            SeaTunnelRowType sourceType = upstreamDataStreams.get(0).getCatalogTable().getSeaTunnelRowType();
            transform.setJobContext(jobContext);
            DataStream<Row> inputStream =
                    flinkTransform(sourceType, transform, stream);
            registerResultTable(pluginConfig, inputStream);
            return new DataStreamTableInfo(
                            inputStream,
                            transform.getProducedCatalogTable(),
                            pluginConfig.hasPath(RESULT_TABLE_NAME.key())
                                    ? pluginConfig.getString(RESULT_TABLE_NAME.key())
                                    : null);
        } catch (Exception e) {
            throw new TaskExecuteException(
                    String.format(
                            "SeaTunnel transform task: %s execute error",
                            plugin.factoryIdentifier()),
                    e);
        }
    }

    protected DataStream<Row> flinkTransform(
            SeaTunnelRowType sourceType, SeaTunnelTransform transform, DataStream<Row> stream) {
        TypeInformation rowTypeInfo =
                TypeConverterUtils.convert(
                        transform.getProducedCatalogTable().getSeaTunnelRowType());
        FlinkRowConverter transformInputRowConverter = new FlinkRowConverter(sourceType);
        FlinkRowConverter transformOutputRowConverter =
                new FlinkRowConverter(transform.getProducedCatalogTable().getSeaTunnelRowType());
        DataStream<Row> output =
                stream.flatMap(
                        (FlatMapFunction<Row, Row>)
                                (value, out) -> {
                                    SeaTunnelRow seaTunnelRow =
                                            transformInputRowConverter.reconvert(value);
                                    SeaTunnelRow dataRow =
                                            (SeaTunnelRow) transform.map(seaTunnelRow);
                                    if (dataRow != null) {
                                        Row copy = transformOutputRowConverter.convert(dataRow);
                                        out.collect(copy);
                                    }
                                },
                        rowTypeInfo);
        return output;
    }
}
