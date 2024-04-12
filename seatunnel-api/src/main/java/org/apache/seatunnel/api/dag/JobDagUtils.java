package org.apache.seatunnel.api.dag;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.commons.collections4.CollectionUtils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * 任务DAG解析工具
 *
 * @Author: Feng
 * @Date: 2023/11/23 11:33
 */
public class JobDagUtils {

    /**
     * 基于配置，初始化DAG配置，构建链接
     *
     * @param config
     * @return
     */
    public static List<JobDagNode> createDagNodeChainByConfig(Config config) {
        List<? extends Config> configList = config.getConfigList("dag");
        Map<String, JobDagNode> idJobDagNodeMap = new HashMap<>();
        List<JobDagNode> jobDagNodes =
                configList.stream()
                        .map(c -> {
                            JobDagNode dagNode = initJobDagNode(c);
                            idJobDagNodeMap.put(dagNode.getId(), dagNode);
                            return dagNode;
                        }).collect(Collectors.toList());
        checkArgument(CollectionUtils.isNotEmpty(jobDagNodes), "dag节点不能为空");
        jobDagNodes.forEach(dagNode -> {
            dagNode.setLasts(new HashSet<>());
            dagNode.lastIds().forEach(id -> dagNode.getLasts().add(idJobDagNodeMap.get(id)));
            dagNode.setNexts(new HashSet<>());
            dagNode.nextIds().forEach(id -> dagNode.getNexts().add(idJobDagNodeMap.get(id)));
        });
        return jobDagNodes;
    }

    /**
     * 基本的初始化
     * @param config
     * @return
     */
    private static JobDagNode initJobDagNode(Config config) {
        JobDagNode jobDagNode = new JobDagNode();
        jobDagNode.initBasic(config);
        return jobDagNode;
    }
}
