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

package org.apache.seatunnel.core.starter.spark.execution;

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
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.core.starter.enums.PluginType;
import org.apache.seatunnel.core.starter.exception.TaskExecuteException;
import org.apache.seatunnel.core.starter.execution.PluginUtil;
import org.apache.seatunnel.plugin.discovery.PluginIdentifier;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelFactoryDiscovery;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelSinkPluginDiscovery;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.translation.spark.sink.SparkSinkInjector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;

import static org.apache.seatunnel.api.common.CommonOptions.PLUGIN_NAME;
import static org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode.HANDLE_SAVE_MODE_FAILED;

public class SinkExecuteProcessor
        extends SparkAbstractPluginExecuteProcessor<Optional<? extends Factory>> {
    private static final String PLUGIN_TYPE = PluginType.SINK.getType();

    protected SinkExecuteProcessor(
            SparkRuntimeEnvironment sparkRuntimeEnvironment,
            JobContext jobContext,
            Config pluginConfig) {
        super(sparkRuntimeEnvironment, jobContext, pluginConfig);
    }

    @Override
    protected Optional<? extends Factory> initializePlugin() {
        SeaTunnelFactoryDiscovery factoryDiscovery = new SeaTunnelFactoryDiscovery(TableSinkFactory.class);
        SeaTunnelSinkPluginDiscovery sinkPluginDiscovery = new SeaTunnelSinkPluginDiscovery();
        List<URL> pluginJars = new ArrayList<>();
        sparkRuntimeEnvironment.registerPlugin(pluginJars);
        return PluginUtil.createSinkFactory(
                factoryDiscovery,
                sinkPluginDiscovery,
                pluginConfig,
                new HashSet<>());
    }

    @Override
    public DatasetTableInfo execute(List<DatasetTableInfo> upstreamDataStreams)
            throws TaskExecuteException {
        Optional<Dataset<Row>> reduce = upstreamDataStreams.stream()
                .map(t -> t.getDataset()).reduce((t1, t2) -> t1.union(t2));
        if (!reduce.isPresent()) {
            throw new TaskExecuteException("该节点没有上游节点！");
        }
        Dataset<Row> stream = reduce.get();

        SeaTunnelSinkPluginDiscovery sinkPluginDiscovery = new SeaTunnelSinkPluginDiscovery();
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

        SeaTunnelDataType<?> inputType = upstreamDataStreams.get(0).getCatalogTable().getSeaTunnelRowType();

        int parallelism;
        if (pluginConfig.hasPath(CommonOptions.PARALLELISM.key())) {
            parallelism = pluginConfig.getInt(CommonOptions.PARALLELISM.key());
        } else {
            parallelism =
                    sparkRuntimeEnvironment
                            .getSparkConf()
                            .getInt(
                                    CommonOptions.PARALLELISM.key(),
                                    CommonOptions.PARALLELISM.defaultValue());
        }
        stream.sparkSession().read().option(CommonOptions.PARALLELISM.key(), parallelism);
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
            sink.setTypeInfo((SeaTunnelRowType) inputType);
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
        // TODO modify checkpoint location
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
        String applicationId =
                sparkRuntimeEnvironment.getSparkSession().sparkContext().applicationId();
        SparkSinkInjector.inject(
                        stream.write(),
                        sink,
                        upstreamDataStreams.get(0).getCatalogTable(),
                        applicationId)
                .option("checkpointLocation", "/tmp")
                .save();
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
