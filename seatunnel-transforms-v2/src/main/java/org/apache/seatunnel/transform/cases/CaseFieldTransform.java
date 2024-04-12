package org.apache.seatunnel.transform.cases;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.transform.common.AbstractCatalogSupportTransform;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @Date: 2023/11/28 14:58
 */
@Slf4j
public class CaseFieldTransform extends AbstractCatalogSupportTransform {
    public static final String PLUGIN_NAME = "Case";

    private Set<Integer> upperCaseFieldIndex = new HashSet<>();
    private Set<Integer> lowerCaseFieldIndex = new HashSet<>();
    private List<String> upperCaseFieldName;
    private List<String> lowerCaseFieldName;

    public CaseFieldTransform(@NonNull ReadonlyConfig config, @NonNull CatalogTable catalogTable) {
        super(catalogTable);
        SeaTunnelRowType seaTunnelRowType = catalogTable.getTableSchema().toPhysicalRowDataType();
        upperCaseFieldName = config.get(CaseFieldTransformConfig.UPPER_FIELDS);
        lowerCaseFieldName = config.get(CaseFieldTransformConfig.LOWER_FIELDS);

        if (upperCaseFieldName != null) {
            upperCaseFieldName.stream().forEach(upperField -> {
                int index = seaTunnelRowType.indexOf(upperField);
                if (index == -1) {
                    throw new IllegalArgumentException("找不到[" + upperField + "]字段");
                }
                upperCaseFieldIndex.add(index);
            });
        }
        if (lowerCaseFieldName != null) {
            lowerCaseFieldName.stream().forEach(lowerField -> {
                int index = seaTunnelRowType.indexOf(lowerField);
                if (index == -1) {
                    throw new IllegalArgumentException("找不到[" + lowerField + "]字段");
                }
                lowerCaseFieldIndex.add(index);
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
            if (upperCaseFieldIndex.contains(i)) {
                values[i] = value.toString().toUpperCase();
            } else if (lowerCaseFieldIndex.contains(i)) {
                values[i] = value.toString().toLowerCase();
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
