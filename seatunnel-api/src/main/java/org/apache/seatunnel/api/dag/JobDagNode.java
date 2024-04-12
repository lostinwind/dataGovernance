package org.apache.seatunnel.api.dag;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import lombok.Data;

import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * 任务DAG节点
 * @Author: Feng
 * @Date: 2023/11/23 11:22
 */
@Data
public class JobDagNode {

    private String id;
    private String code;
    private String type;
    private Integer level;

    private Set<JobDagNode> lasts = new HashSet<>();
    private Set<JobDagNode> nexts = new HashSet<>();

    private Config config;

    public void initBasic(Config config) {
        this.config = config;
        this.id = config.getString("id");
        this.code = config.getString("code");
        this.type = config.getString("type");
        this.level = config.getInt("level");
    }

    public Set<String> nextIds() {
        if (config == null) {
            return Collections.EMPTY_SET;
        }
        return new HashSet<>(config.getStringList("nexts"));
    }

    public Set<String> lastIds() {
        if (config == null) {
            return Collections.EMPTY_SET;
        }
        return new HashSet<>(config.getStringList("lasts"));
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, code, type, level);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof JobDagNode)) {
            return false;
        }
        JobDagNode that = (JobDagNode) obj;
        return Objects.equals(this.id, that.id) && Objects.equals(this.code, that.code)
                && Objects.equals(this.type, that.type) && Objects.equals(this.level, that.level);
    }

    @Override
    public String toString() {
        return String.format(
                "id: %s, code: %s, type: %s, level: %s", id, code, type, level);
    }
}
