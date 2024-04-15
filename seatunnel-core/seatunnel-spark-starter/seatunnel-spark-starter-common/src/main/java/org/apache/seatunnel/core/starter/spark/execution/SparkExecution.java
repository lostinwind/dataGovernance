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

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.dag.JobDagNode;
import org.apache.seatunnel.api.dag.JobDagUtils;
import org.apache.seatunnel.core.starter.exception.TaskExecuteException;
import org.apache.seatunnel.core.starter.execution.PluginExecuteProcessor;
import org.apache.seatunnel.core.starter.execution.RuntimeEnvironment;
import org.apache.seatunnel.core.starter.execution.TaskExecution;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class SparkExecution implements TaskExecution {
    private final SparkRuntimeEnvironment sparkRuntimeEnvironment;
    private final List<JobDagNode> sourceDagNodes;
    private final Map<String, PluginExecuteProcessor> pluginExecuteProcessorMap;

    public SparkExecution(Config config) {
        this.sparkRuntimeEnvironment = SparkRuntimeEnvironment.getInstance(config);
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
    }

    private void initExecuteProcessor(JobDagNode dagNode, JobContext jobContext) throws Exception {
        PluginExecuteProcessor pluginExecuteProcessor = null;
        if (StringUtils.equalsIgnoreCase("input", dagNode.getType())) {
            pluginExecuteProcessor = new SourceExecuteProcessor(sparkRuntimeEnvironment, jobContext, dagNode.getConfig());
        } else if (StringUtils.equalsIgnoreCase("transition", dagNode.getType())) {
            pluginExecuteProcessor = new TransformExecuteProcessor(sparkRuntimeEnvironment, jobContext, dagNode.getConfig());
        } else if (StringUtils.equalsIgnoreCase("output", dagNode.getType())) {
            pluginExecuteProcessor = new SinkExecuteProcessor(sparkRuntimeEnvironment, jobContext, dagNode.getConfig());
        }
        pluginExecuteProcessorMap.put(dagNode.getId(), pluginExecuteProcessor);
    }

    @Override
    public void execute() throws TaskExecuteException {
        Map<String, DatasetTableInfo> dataNodeIdDatasetMap = new HashMap<>();
        sourceDagNodes.stream().forEach(dagNode -> {
            try {
                handlePluginExecute(dagNode, dataNodeIdDatasetMap);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        log.info("Spark Execution started");
    }

    private void handlePluginExecute(JobDagNode dagNode,
                                     Map<String, DatasetTableInfo> dataNodeIdDatasetMap) throws Exception {
        DatasetTableInfo datasetTableInfo = dataNodeIdDatasetMap.get(dagNode.getId());
        if (datasetTableInfo != null) return;
        PluginExecuteProcessor<DatasetTableInfo, SparkRuntimeEnvironment> pluginExecuteProcessor =
                pluginExecuteProcessorMap.get(dagNode.getId());
        if (dagNode.getType().equals("input")) { // 生产dataset
            dataNodeIdDatasetMap.put(dagNode.getId(), pluginExecuteProcessor.execute(null));
        } else {
            List<DatasetTableInfo> datasetTableInfos = new ArrayList<>();
            for (JobDagNode last : dagNode.getLasts()) {
                DatasetTableInfo lastDatasetTableInfo = dataNodeIdDatasetMap.get(last.getId());
                if (lastDatasetTableInfo == null) {
                    return;
                } else {
                    datasetTableInfos.add(lastDatasetTableInfo);
                }
            }
            dataNodeIdDatasetMap.put(dagNode.getId(), pluginExecuteProcessor.execute(datasetTableInfos));
        }
        for (JobDagNode nextNode : dagNode.getNexts()) {
            handlePluginExecute(nextNode, dataNodeIdDatasetMap);
        }
    }

    public SparkRuntimeEnvironment getSparkRuntimeEnvironment() {
        return sparkRuntimeEnvironment;
    }
}
