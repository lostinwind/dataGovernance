package org.apache.seatunnel.transform.concat;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.transform.common.AbstractCatalogSupportTransform;
import scala.Tuple2;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author: Feng
 * @Date: 2023/11/28 16:24
 */
@Slf4j
public class ConcatFieldTransform extends AbstractCatalogSupportTransform {
    public static final String PLUGIN_NAME = "Concat";
    private List<Map> concatFields;
    private Map<Integer, Tuple2<String, String>> concatIndexFields = new HashMap<>();

    public ConcatFieldTransform(@NonNull ReadonlyConfig config, @NonNull CatalogTable catalogTable) {
        super(catalogTable);
        SeaTunnelRowType seaTunnelRowType = catalogTable.getTableSchema().toPhysicalRowDataType();
        concatFields = config.get(ConcatFieldTransformConfig.FIELDS);
        if (concatFields != null) {
            concatFields.stream().forEach(field -> {
                String fieldName = (String) field.get("column");
                String left = (String) field.get("left");
                String right = (String) field.get("right");
                int index = seaTunnelRowType.indexOf(fieldName);
                if (index == -1) {
                    throw new IllegalArgumentException("找不到[" + fieldName + "]字段");
                }
                Tuple2<String, String> configPair = new Tuple2<>(left, right);
                concatIndexFields.put(index, configPair);
            });
        }
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    protected SeaTunnelRow transformRow(SeaTunnelRow inputRow) {
        Object[] values = new Object[inputRow.getFields().length];
        for (int i = 0; i < values.length; i++) {
            Object value = inputRow.getField(i);
            if (value == null) {
                values[i] = null;
                continue;
            }
            Tuple2<String, String> config = concatIndexFields.get(i);
            if (config != null) {
                if (config._1() != null && config._2() != null) {
                    values[i] = String.format("%s%s%s", config._1(), value, config._2());
                } else if (config._1() != null) {
                    values[i] = String.format("%s%s", config._1(), value);
                } else if (config._2() != null) {
                    values[i] = String.format("%s%s", value, config._2());
                }
            } else {
                values[i] = value;
            }
        }
        return new SeaTunnelRow(values);
    }

    @Override
    protected TableSchema transformTableSchema() {
        return inputCatalogTable.getTableSchema().copy();
    }

    @Override
    protected TableIdentifier transformTableIdentifier() {
        return inputCatalogTable.getTableId().copy();
    }
}
