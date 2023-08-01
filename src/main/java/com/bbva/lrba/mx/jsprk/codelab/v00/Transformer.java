package com.bbva.lrba.mx.jsprk.codelab.v00;

import com.bbva.lrba.spark.transformers.Transform;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.HashMap;
import java.util.Map;

import static org.apache.spark.sql.functions.*;

public class Transformer implements Transform {

    @Override
    public Map<String, Dataset<Row>> transform(Map<String, Dataset<Row>> datasetsFromRead) {
        Map<String, Dataset<Row>> datasetsToWrite = new HashMap<>();

        Dataset<Row> dataset1 = datasetsFromRead.get("sourceAlias1");
        Dataset<Row> dataset2 = datasetsFromRead.get("sourceAlias2");
        dataset1.show();
        dataset2.show();

        System.out.println("Dataset1 Counting: " + dataset1.count());
        System.out.println("Dataset2 Counting: " + dataset2.count());

        Dataset<Row> joinDNIDataset = dataset1.join(dataset2, "DNI");

        Dataset<Row> exerciseDataset = joinDNIDataset.withColumn("FECHA", lit(current_date()))
                .withColumn("DNI_CONDITION", when(col("DNI").equalTo(1), 200).otherwise(40))
                .withColumn("DNI", col("DNI").cast("Integer"));

        datasetsToWrite.put("joinDNIDataset", exerciseDataset);

        System.out.println("Impresion de joinDNIDataset en class Transformer:");
        datasetsToWrite.get("joinDNIDataset").show();

        exerciseDataset.groupBy("FECHA").agg(sum("DNI"),avg("DNI"), max("DNI"))
                                             .drop("FECHA").show();

        return datasetsToWrite;
    }
}