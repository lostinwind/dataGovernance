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

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.transformations.SinkV1Adapter;
import org.apache.flink.types.Row;
import org.apache.seatunnel.api.common.CommonOptions;
import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.sink.SaveModeExecuteWrapper;
import org.apache.seatunnel.api.sink.SaveModeHandler;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SupportSaveMode;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.core.starter.enums.PluginType;
import org.apache.seatunnel.core.starter.exception.TaskExecuteException;
import org.apache.seatunnel.core.starter.execution.PluginUtil;
import org.apache.seatunnel.plugin.discovery.PluginIdentifier;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelFactoryDiscovery;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelSinkPluginDiscovery;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.translation.flink.sink.FlinkSink;

import java.net.URL;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.apache.seatunnel.api.common.CommonOptions.PLUGIN_NAME;
import static org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode.HANDLE_SAVE_MODE_FAILED;

public class SinkExecuteProcessor
        extends FlinkAbstractPluginExecuteProcessor<Optional<? extends Factory>> {

    private static final String PLUGIN_TYPE = PluginType.SINK.getType();

    protected SinkExecuteProcessor(
            Config pluginConfig,
            JobContext jobContext,
            Set<URL> jarPaths,
            Config envConfig) {
        super(pluginConfig, jobContext, jarPaths, envConfig);
    }

    @Override
    protected Optional<? extends Factory> initializePlugin() {
        SeaTunnelFactoryDiscovery factoryDiscovery =
                new SeaTunnelFactoryDiscovery(TableSinkFactory.class, ADD_URL_TO_CLASSLOADER);
        SeaTunnelSinkPluginDiscovery sinkPluginDiscovery = new SeaTunnelSinkPluginDiscovery(ADD_URL_TO_CLASSLOADER);
        return PluginUtil.createSinkFactory(
                factoryDiscovery,
                sinkPluginDiscovery,
                pluginConfig,
                jarPaths);
    }

    @Override
    public DataStreamTableInfo execute(List<DataStreamTableInfo> upstreamDataStreams) throws TaskExecuteException {
        Optional<DataStream<Row>> reduce = upstreamDataStreams.stream()
                .map(t -> t.getDataStream()).reduce((t1, t2) -> t1.union(t2));
        if (!reduce.isPresent()) {
            throw new TaskExecuteException("该节点没有上游节点！");
        }
        DataStream<Row> stream = reduce.get();

        SeaTunnelSinkPluginDiscovery sinkPluginDiscovery =
                new SeaTunnelSinkPluginDiscovery(ADD_URL_TO_CLASSLOADER);
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

        Optional<? extends Factory> factory = plugin;
        boolean fallBack = !factory.isPresent() || isFallback(factory.get());
        SeaTunnelSink sink;
        if (fallBack) {
            sink =
                    fallbackCreateSink(
                            sinkPluginDiscovery,
                            PluginIdentifier.of(
                                    ENGINE_TYPE,
                                    PLUGIN_TYPE,
                                    pluginConfig.getString(PLUGIN_NAME.key())),
                            pluginConfig);
            sink.setJobContext(jobContext);
            SeaTunnelRowType sourceType = upstreamDataStreams.get(0).getCatalogTable().getSeaTunnelRowType();
            sink.setTypeInfo(sourceType);
        } else {
            TableSinkFactoryContext context =
                    new TableSinkFactoryContext(
                            upstreamDataStreams.get(0).getCatalogTable(),
                            ReadonlyConfig.fromConfig(pluginConfig),
                            classLoader);
            ConfigValidator.of(context.getOptions()).validate(factory.get().optionRule());
            sink = ((TableSinkFactory) factory.get()).createSink(context).createSink();
            sink.setJobContext(jobContext);
        }
        if (SupportSaveMode.class.isAssignableFrom(sink.getClass())) {
            SupportSaveMode saveModeSink = (SupportSaveMode) sink;
            Optional<SaveModeHandler> saveModeHandler = saveModeSink.getSaveModeHandler();
            if (saveModeHandler.isPresent()) {
                try (SaveModeHandler handler = saveModeHandler.get()) {
                    new SaveModeExecuteWrapper(handler).execute();
                } catch (Exception e) {
                    throw new SeaTunnelRuntimeException(HANDLE_SAVE_MODE_FAILED, e);
                }
            }
        }
        DataStreamSink<Row> dataStreamSink =
                stream.sinkTo(
                                SinkV1Adapter.wrap(
                                        new FlinkSink<>(sink, upstreamDataStreams.get(0).getCatalogTable())))
                        .name(sink.getPluginName());
        if (pluginConfig.hasPath(CommonOptions.PARALLELISM.key())) {
            int parallelism = pluginConfig.getInt(CommonOptions.PARALLELISM.key());
            dataStreamSink.setParallelism(parallelism);
        }
        // the sink is the last stream
        return null;
    }

    public boolean isFallback(Factory factory) {
        try {
            ((TableSinkFactory) factory).createSink(null);
        } catch (Exception e) {
            if (e instanceof UnsupportedOperationException
                    && "The Factory has not been implemented and the deprecated Plugin will be used."
                            .equals(e.getMessage())) {
                return true;
            }
        }
        return false;
    }

    public SeaTunnelSink fallbackCreateSink(
            SeaTunnelSinkPluginDiscovery sinkPluginDiscovery,
            PluginIdentifier pluginIdentifier,
            Config pluginConfig) {
        SeaTunnelSink source = sinkPluginDiscovery.createPluginInstance(pluginIdentifier);
        source.prepare(pluginConfig);
        return source;
    }
}
