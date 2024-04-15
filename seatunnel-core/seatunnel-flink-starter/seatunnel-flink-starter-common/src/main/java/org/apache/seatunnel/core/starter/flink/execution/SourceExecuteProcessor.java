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

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.seatunnel.api.common.CommonOptions;
import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SupportCoordinate;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.core.starter.enums.PluginType;
import org.apache.seatunnel.core.starter.execution.PluginUtil;
import org.apache.seatunnel.core.starter.execution.SourceTableInfo;
import org.apache.seatunnel.plugin.discovery.PluginIdentifier;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelFactoryDiscovery;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelSourcePluginDiscovery;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.translation.flink.source.FlinkSource;

import java.net.URL;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.seatunnel.api.common.CommonOptions.PLUGIN_NAME;
import static org.apache.seatunnel.api.common.CommonOptions.RESULT_TABLE_NAME;

@Slf4j
@SuppressWarnings("unchecked,rawtypes")
public class SourceExecuteProcessor extends FlinkAbstractPluginExecuteProcessor<SourceTableInfo> {
    private static final String PLUGIN_TYPE = PluginType.SOURCE.getType();

    public SourceExecuteProcessor(
            Config pluginConfig,
            JobContext jobContext,
            Set<URL> jarPaths,
            Config envConfig) {
        super(pluginConfig, jobContext, jarPaths, envConfig);
    }

    @Override
    public DataStreamTableInfo execute(List<DataStreamTableInfo> upstreamDataStreams) {
        StreamExecutionEnvironment executionEnvironment =
                flinkRuntimeEnvironment.getStreamExecutionEnvironment();

        SourceTableInfo sourceTableInfo = plugin;
        SeaTunnelSource internalSource = sourceTableInfo.getSource();
        if (internalSource instanceof SupportCoordinate) {
            registerAppendStream(pluginConfig);
        }
        FlinkSource flinkSource = new FlinkSource<>(internalSource, envConfig);

        DataStreamSource sourceStream =
                executionEnvironment.fromSource(
                        flinkSource,
                        WatermarkStrategy.noWatermarks(),
                        String.format("%s-source", internalSource.getPluginName()));

        if (pluginConfig.hasPath(CommonOptions.PARALLELISM.key())) {
            int parallelism = pluginConfig.getInt(CommonOptions.PARALLELISM.key());
            sourceStream.setParallelism(parallelism);
        }
        registerResultTable(pluginConfig, sourceStream);
        return new DataStreamTableInfo(
                        sourceStream,
                        sourceTableInfo.getCatalogTables().get(0),
                        pluginConfig.hasPath(RESULT_TABLE_NAME.key())
                                ? pluginConfig.getString(RESULT_TABLE_NAME.key())
                                : null);
    }

    @Override
    protected SourceTableInfo initializePlugin() {
        SeaTunnelSourcePluginDiscovery sourcePluginDiscovery =
                new SeaTunnelSourcePluginDiscovery(ADD_URL_TO_CLASSLOADER);

        SeaTunnelFactoryDiscovery factoryDiscovery =
                new SeaTunnelFactoryDiscovery(TableSourceFactory.class, ADD_URL_TO_CLASSLOADER);

        Set<URL> jars = new HashSet<>();
        PluginIdentifier pluginIdentifier =
                PluginIdentifier.of(ENGINE_TYPE, PLUGIN_TYPE, pluginConfig.getString(PLUGIN_NAME.key()));
        jars.addAll(sourcePluginDiscovery.getPluginJarPaths(Lists.newArrayList(pluginIdentifier)));
        SourceTableInfo source =
                PluginUtil.createSource(
                        factoryDiscovery,
                        sourcePluginDiscovery,
                        pluginIdentifier,
                        pluginConfig,
                        jobContext);
        jarPaths.addAll(jars);
        return source;
    }
}
