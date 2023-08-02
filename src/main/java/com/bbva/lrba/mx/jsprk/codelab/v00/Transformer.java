package com.bbva.lrba.mx.jsprk.codelab.v00;

import com.bbva.lrba.spark.transformers.Transform;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.HashMap;
import java.util.Map;

import static com.bbva.lrba.mx.jsprk.codelab.v00.utils.Constants.*;

public class Transformer implements Transform {

    @Override
    public Map<String, Dataset<Row>> transform(Map<String, Dataset<Row>> datasetsFromRead) {
        Map<String, Dataset<Row>> datasetsToWrite = new HashMap<>();

        Dataset<Row> dataset1 = datasetsFromRead.get(ALIAS_TABLE_ONE.getValue());
        Dataset<Row> datasetNew = datasetsFromRead.get(ALIAS_TABLE_THREE.getValue());

        Dataset<Row> dataSet3 = datasetNew.join(dataset1, ENTIDAD_COLUMN.getValue())
                .dropDuplicates(NOMBRE_COLUMN.getValue());
        dataSet3.show();

        dataSet3 = dataSet3.drop(dataSet3.col(DNI_COLUMN.getValue()));
        datasetsToWrite.put("joinDataset31", dataSet3);

        return datasetsToWrite;
    }
}