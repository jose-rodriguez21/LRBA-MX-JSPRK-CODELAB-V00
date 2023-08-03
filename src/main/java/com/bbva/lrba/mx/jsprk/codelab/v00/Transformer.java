package com.bbva.lrba.mx.jsprk.codelab.v00;

import com.bbva.lrba.spark.transformers.Transform;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import java.util.HashMap;
import java.util.Map;

import static com.bbva.lrba.mx.jsprk.codelab.v00.utils.Constants.*;

public class Transformer implements Transform {

    @Override
    public Map<String, Dataset<Row>> transform(Map<String, Dataset<Row>> datasetsFromRead) {
        Map<String, Dataset<Row>> datasetsToWrite = new HashMap<>();

        Dataset<Row> dataset1 = datasetsFromRead.get(ALIAS_TABLE_ONE.getValue());
        Dataset<Row> dataset2 = datasetsFromRead.get(ALIAS_TABLE_TWO.getValue());
        Dataset<Row> datasetNew = datasetsFromRead.get(ALIAS_TABLE_THREE.getValue());

        // Ejercicio 31 de Julio de 2023
        System.out.println("Dataset1 Counting: " + dataset1.count());
        System.out.println("Dataset2 Counting: " + dataset2.count());

        Dataset<Row> joinDNIDataset = dataset1.join(dataset2, DNI_COLUMN.getValue());

        Dataset<Row> exerciseDataset = joinDNIDataset.withColumn(FECHA_COLUMN.getValue(), functions.lit(functions.current_date()))
                .withColumn(DNI_CONDITION_COLUMN.getValue(), functions.when(functions.col(DNI_COLUMN.getValue()).equalTo(1), 200).otherwise(40))
                .withColumn(DNI_COLUMN.getValue(), functions.col(DNI_COLUMN.getValue()).cast("Integer"));

        System.out.println("Impresion de joinDNIDataset en class Transformer:");
        exerciseDataset.show();

        datasetsToWrite.put("joinDNIDataset", exerciseDataset);

        exerciseDataset.groupBy(FECHA_COLUMN.getValue())
                .agg(functions.sum(DNI_COLUMN.getValue()).alias("suma"),
                        functions.avg(DNI_COLUMN.getValue()).alias("promedio"),
                        functions.max(DNI_COLUMN.getValue()).alias("maximo"))
                .drop(FECHA_COLUMN.getValue()).show();

        // Ejercicio 2 de Agosto de 2023
        Dataset<Row> dataSet3 = datasetNew.join(dataset1, ENTIDAD_COLUMN.getValue())
                .dropDuplicates(NOMBRE_COLUMN.getValue());
        dataSet3.show();

        dataSet3 = dataSet3.drop(dataSet3.col(DNI_COLUMN.getValue()));
        datasetsToWrite.put("joinDataset31", dataSet3);

        return datasetsToWrite;
    }
}