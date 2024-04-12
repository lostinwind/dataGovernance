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

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.dag.JobDagNode;
import org.apache.seatunnel.api.dag.JobDagUtils;
import org.apache.seatunnel.api.env.EnvCommonOptions;
import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.core.starter.exception.TaskExecuteException;
import org.apache.seatunnel.core.starter.execution.PluginExecuteProcessor;
import org.apache.seatunnel.core.starter.execution.RuntimeEnvironment;
import org.apache.seatunnel.core.starter.execution.TaskExecution;
import org.apache.seatunnel.core.starter.flink.FlinkStarter;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigUtil;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValueFactory;
import org.apache.seatunnel.translation.flink.metric.FlinkJobMetricsSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Used to execute a SeaTunnelTask. */
public class FlinkExecution implements TaskExecution {

    private static final Logger LOGGER = LoggerFactory.getLogger(FlinkExecution.class);

    private final FlinkRuntimeEnvironment flinkRuntimeEnvironment;
    private final List<JobDagNode> sourceDagNodes;
    private final Map<String, PluginExecuteProcessor> pluginExecuteProcessorMap;
    private final Set<URL> jarPaths;
    private final Config envConfig;

    public FlinkExecution(Config config) {
        try {
            jarPaths =
                    new HashSet<>(
                            Collections.singletonList(
                                    new File(
                                                    Common.appStarterDir()
                                                            .resolve(FlinkStarter.APP_JAR_NAME)
                                                            .toString())
                                            .toURI()
                                            .toURL()));
        } catch (MalformedURLException e) {
            throw new SeaTunnelException("load flink starter error.", e);
        }
        envConfig = config.getConfig("env");
        registerPlugin(envConfig);
        JobContext jobContext = new JobContext();
        jobContext.setJobMode(RuntimeEnvironment.getJobMode(config));

        List<JobDagNode> dagNodeList = JobDagUtils.createDagNodeChainByConfig(config);
        sourceDagNodes = dagNodeList.stream()
                .filter(node -> "input".equals(node.getType()))
                .collect(Collectors.toList());

        pluginExecuteProcessorMap = new HashMap<>();
        dagNodeList.forEach(node -> {
            try {
                initExecuteProcessor(node, jobContext);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

//        this.flinkRuntimeEnvironment = FlinkRuntimeEnvironment.getInstance(this.registerPlugin(config, jarPaths));
        this.flinkRuntimeEnvironment = FlinkRuntimeEnvironment.getInstance(config);
        for (PluginExecuteProcessor pluginExecuteProcessor : pluginExecuteProcessorMap.values()) {
            pluginExecuteProcessor.setRuntimeEnvironment(flinkRuntimeEnvironment);
        }
    }

    private void initExecuteProcessor(JobDagNode dagNode, JobContext jobContext) throws Exception {
        PluginExecuteProcessor pluginExecuteProcessor = null;
        if (StringUtils.equalsIgnoreCase("input", dagNode.getType())) {
            pluginExecuteProcessor = new SourceExecuteProcessor(dagNode.getConfig(), jobContext, jarPaths, envConfig);
        } else if (StringUtils.equalsIgnoreCase("transition", dagNode.getType())) {
            pluginExecuteProcessor = new TransformExecuteProcessor(dagNode.getConfig(), jobContext, jarPaths, envConfig);
        } else if (StringUtils.equalsIgnoreCase("output", dagNode.getType())) {
            pluginExecuteProcessor = new SinkExecuteProcessor(dagNode.getConfig(), jobContext, jarPaths, envConfig);
        }
        pluginExecuteProcessorMap.put(dagNode.getId(), pluginExecuteProcessor);
    }

    @Override
    public void execute() throws TaskExecuteException {
        Map<String, DataStreamTableInfo> dataNodeIdDatasetMap = new HashMap<>();
        sourceDagNodes.stream().forEach(dagNode -> {
            try {
                handlePluginExecute(dagNode, dataNodeIdDatasetMap);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        try {
            LOGGER.info(
                    "Flink Execution Plan: {}",
                    flinkRuntimeEnvironment.getStreamExecutionEnvironment().getExecutionPlan());
            LOGGER.info("Flink job name: {}", flinkRuntimeEnvironment.getJobName());
            if (!flinkRuntimeEnvironment.isStreaming()) {
                flinkRuntimeEnvironment
                        .getStreamExecutionEnvironment()
                        .setRuntimeMode(RuntimeExecutionMode.BATCH);
                LOGGER.info("Flink job Mode: {}", JobMode.BATCH);
            }

            final long jobStartTime = System.currentTimeMillis();
            JobExecutionResult jobResult =
                    flinkRuntimeEnvironment
                            .getStreamExecutionEnvironment()
                            .execute(flinkRuntimeEnvironment.getJobName());
            final long jobEndTime = System.currentTimeMillis();

            final FlinkJobMetricsSummary jobMetricsSummary =
                    FlinkJobMetricsSummary.builder()
                            .jobExecutionResult(jobResult)
                            .jobStartTime(jobStartTime)
                            .jobEndTime(jobEndTime)
                            .build();

            LOGGER.info("Job finished, execution result: \n{}", jobMetricsSummary);
        } catch (Exception e) {
            throw new RuntimeException("Execute Flink job error", e);
        }
    }

    private void handlePluginExecute(JobDagNode dagNode,
                                     Map<String, DataStreamTableInfo> dataNodeIdDatasetMap) throws Exception {
        DataStreamTableInfo dataStreamTableInfo = dataNodeIdDatasetMap.get(dagNode.getId());
        if (dataStreamTableInfo != null) return;
        PluginExecuteProcessor<DataStreamTableInfo, FlinkRuntimeEnvironment> pluginExecuteProcessor =
                pluginExecuteProcessorMap.get(dagNode.getId());
        if (dagNode.getType().equals("input")) { // 生产dataset
            dataNodeIdDatasetMap.put(dagNode.getId(), pluginExecuteProcessor.execute(null));
        } else {
            List<DataStreamTableInfo> dataStreamTableInfos = new ArrayList<>();
            for (JobDagNode last : dagNode.getLasts()) {
                DataStreamTableInfo lastDataStreamTableInfo = dataNodeIdDatasetMap.get(last.getId());
                if (lastDataStreamTableInfo == null) {
                    return;
                } else {
                    dataStreamTableInfos.add(lastDataStreamTableInfo);
                }
            }
            dataNodeIdDatasetMap.put(dagNode.getId(), pluginExecuteProcessor.execute(dataStreamTableInfos));
        }
        for (JobDagNode nextNode : dagNode.getNexts()) {
            handlePluginExecute(nextNode, dataNodeIdDatasetMap);
        }
    }

    private void registerPlugin(Config envConfig) {
        List<Path> thirdPartyJars = new ArrayList<>();
        if (envConfig.hasPath(EnvCommonOptions.JARS.key())) {
            thirdPartyJars =
                    new ArrayList<>(
                            Common.getThirdPartyJars(
                                    envConfig.getString(EnvCommonOptions.JARS.key())));
        }
        thirdPartyJars.addAll(Common.getPluginsJarDependencies());
        List<URL> jarDependencies =
                Stream.concat(thirdPartyJars.stream(), Common.getLibJars().stream())
                        .map(Path::toUri)
                        .map(
                                uri -> {
                                    try {
                                        return uri.toURL();
                                    } catch (MalformedURLException e) {
                                        throw new RuntimeException(
                                                "the uri of jar illegal:" + uri, e);
                                    }
                                })
                        .collect(Collectors.toList());
        jarDependencies.forEach(
                url ->
                        FlinkAbstractPluginExecuteProcessor.ADD_URL_TO_CLASSLOADER.accept(
                                Thread.currentThread().getContextClassLoader(), url));
        jarPaths.addAll(jarDependencies);
    }

    private Config registerPlugin(Config config, Set<URL> jars) {
        config =
                this.injectJarsToConfig(
                        config, ConfigUtil.joinPath("env", "pipeline", "jars"), jars);
        return this.injectJarsToConfig(
                config, ConfigUtil.joinPath("env", "pipeline", "classpaths"), jars);
    }

    private Config injectJarsToConfig(Config config, String path, Set<URL> jars) {
        List<URL> validJars = new ArrayList<>();
        for (URL jarUrl : jars) {
            if (new File(jarUrl.getFile()).exists()) {
                validJars.add(jarUrl);
                LOGGER.info("Inject jar to config: {}", jarUrl);
            } else {
                LOGGER.warn("Remove invalid jar when inject jars into config: {}", jarUrl);
            }
        }

        if (config.hasPath(path)) {
            Set<URL> paths =
                    Arrays.stream(config.getString(path).split(";"))
                            .map(
                                    uri -> {
                                        try {
                                            return new URL(uri);
                                        } catch (MalformedURLException e) {
                                            throw new RuntimeException(
                                                    "the uri of jar illegal:" + uri, e);
                                        }
                                    })
                            .collect(Collectors.toSet());
            paths.addAll(validJars);

            config =
                    config.withValue(
                            path,
                            ConfigValueFactory.fromAnyRef(
                                    paths.stream()
                                            .map(URL::toString)
                                            .distinct()
                                            .collect(Collectors.joining(";"))));

        } else {
            config =
                    config.withValue(
                            path,
                            ConfigValueFactory.fromAnyRef(
                                    validJars.stream()
                                            .map(URL::toString)
                                            .distinct()
                                            .collect(Collectors.joining(";"))));
        }
        return config;
    }
}
