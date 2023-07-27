package com.bbva.lrba.mx.jsprk.codelab.v00;

import com.bbva.lrba.spark.transformers.Transform;
import com.bbva.lrba.mx.jsprk.codelab.v00.model.RowData;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;

import javax.xml.crypto.Data;
import java.util.HashMap;
import java.util.Map;

public class Transformer implements Transform {

    @Override
    public Map<String, Dataset<Row>> transform(Map<String, Dataset<Row>> datasetsFromRead) {
        Map<String, Dataset<Row>> datasetsToWrite = new HashMap<>();

        Dataset<Row> dataset1 = datasetsFromRead.get("sourceAlias1");
        Dataset<Row> dataset2 = datasetsFromRead.get("sourceAlias2");

        Dataset<Row> joinDNIDataset = dataset1.join(dataset2, "DNI")
                                              .select("DNI")
                                              .where(dataset1.col("DNI").equalTo("000001"));

        datasetsToWrite.put("joinDNIDataset", joinDNIDataset);

        return datasetsToWrite;
    }

}