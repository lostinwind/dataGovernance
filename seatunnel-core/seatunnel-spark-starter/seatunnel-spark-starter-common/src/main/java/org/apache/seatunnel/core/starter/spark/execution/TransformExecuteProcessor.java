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
import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.factory.TableTransformFactory;
import org.apache.seatunnel.api.table.factory.TableTransformFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.core.starter.exception.TaskExecuteException;
import org.apache.seatunnel.core.starter.execution.PluginUtil;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelTransformPluginDiscovery;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.translation.spark.serialization.SeaTunnelRowConverter;
import org.apache.seatunnel.translation.spark.utils.TypeConverterUtils;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.net.URL;
import java.util.*;

import static org.apache.seatunnel.api.common.CommonOptions.RESULT_TABLE_NAME;

@Slf4j
public class TransformExecuteProcessor
        extends SparkAbstractPluginExecuteProcessor<TableTransformFactory> {

    protected TransformExecuteProcessor(
            SparkRuntimeEnvironment sparkRuntimeEnvironment,
            JobContext jobContext,
            Config pluginConfig) {
        super(sparkRuntimeEnvironment, jobContext, pluginConfig);
    }

    @Override
    protected TableTransformFactory initializePlugin() {
        SeaTunnelTransformPluginDiscovery transformPluginDiscovery =
                new SeaTunnelTransformPluginDiscovery();
        Set<URL> pluginJars = new HashSet<>();
        sparkRuntimeEnvironment.registerPlugin(new ArrayList<>(pluginJars));
        return PluginUtil.createTransformFactory(
                transformPluginDiscovery,
                pluginConfig,
                pluginJars);
    }

    @Override
    public DatasetTableInfo execute(List<DatasetTableInfo> upstreamDataStreams)
            throws TaskExecuteException {
        Optional<Dataset<Row>> reduce = upstreamDataStreams.stream().map(t -> t.getDataset())
                .reduce((t1, t2) -> t1.union(t2));
        if (!reduce.isPresent()) {
            throw new TaskExecuteException("该节点没有上游节点！");
        }
        Dataset<Row> stream = reduce.get();

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

            Dataset<Row> inputDataset = sparkTransform(transform, stream, upstreamDataStreams.get(0).getCatalogTable());
            registerInputTempView(pluginConfig, inputDataset);
            return new DatasetTableInfo(
                            inputDataset,
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

    private Dataset<Row> sparkTransform(SeaTunnelTransform transform, Dataset<Row> stream, CatalogTable catalogTable) {
        SeaTunnelDataType<?> inputDataType = catalogTable.getSeaTunnelRowType();
        SeaTunnelDataType<?> outputDataTYpe =
                transform.getProducedCatalogTable().getSeaTunnelRowType();
        StructType outputSchema = (StructType) TypeConverterUtils.convert(outputDataTYpe);
        SeaTunnelRowConverter inputRowConverter = new SeaTunnelRowConverter(inputDataType);
        SeaTunnelRowConverter outputRowConverter = new SeaTunnelRowConverter(outputDataTYpe);
        ExpressionEncoder<Row> encoder = RowEncoder.apply(outputSchema);
        return stream.mapPartitions(
                        (MapPartitionsFunction<Row, Row>)
                                (Iterator<Row> rowIterator) ->
                                        new TransformIterator(
                                                rowIterator,
                                                transform,
                                                outputSchema,
                                                inputRowConverter,
                                                outputRowConverter),
                        encoder)
                .filter(Objects::nonNull);
    }

    private static class TransformIterator implements Iterator<Row>, Serializable {
        private Iterator<Row> sourceIterator;
        private SeaTunnelTransform<SeaTunnelRow> transform;
        private StructType structType;
        private SeaTunnelRowConverter inputRowConverter;
        private SeaTunnelRowConverter outputRowConverter;

        public TransformIterator(
                Iterator<Row> sourceIterator,
                SeaTunnelTransform<SeaTunnelRow> transform,
                StructType structType,
                SeaTunnelRowConverter inputRowConverter,
                SeaTunnelRowConverter outputRowConverter) {
            this.sourceIterator = sourceIterator;
            this.transform = transform;
            this.structType = structType;
            this.inputRowConverter = inputRowConverter;
            this.outputRowConverter = outputRowConverter;
        }

        @Override
        public boolean hasNext() {
            return sourceIterator.hasNext();
        }

        @Override
        public Row next() {
            try {
                Row row = sourceIterator.next();
                SeaTunnelRow seaTunnelRow =
                        inputRowConverter.reconvert(
                                new SeaTunnelRow(((GenericRowWithSchema) row).values()));
                seaTunnelRow = transform.map(seaTunnelRow);
                if (seaTunnelRow == null) {
                    return null;
                }
                seaTunnelRow = outputRowConverter.convert(seaTunnelRow);
                return new GenericRowWithSchema(seaTunnelRow.getFields(), structType);
            } catch (Exception e) {
                throw new TaskExecuteException("Row convert failed, caused: " + e.getMessage(), e);
            }
        }
    }
}
