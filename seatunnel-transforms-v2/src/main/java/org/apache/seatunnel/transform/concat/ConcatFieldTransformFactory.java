package org.apache.seatunnel.transform.concat;

import com.google.auto.service.AutoService;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.connector.TableTransform;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableTransformFactory;
import org.apache.seatunnel.api.table.factory.TableTransformFactoryContext;
import org.apache.seatunnel.transform.cases.CaseFieldTransform;

import static org.apache.seatunnel.transform.concat.ConcatFieldTransform.PLUGIN_NAME;

/**
 * @Author: Feng
 * @Date: 2024/4/11 13:46
 */
@AutoService(Factory.class)
public class ConcatFieldTransformFactory implements TableTransformFactory {

    @Override
    public String factoryIdentifier() {
        return PLUGIN_NAME;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder().build();
    }

    @Override
    public TableTransform createTransform(TableTransformFactoryContext context) {
        CatalogTable catalogTable = context.getCatalogTables().get(0);
        return () -> new ConcatFieldTransform(context.getOptions(), catalogTable);
    }
}
