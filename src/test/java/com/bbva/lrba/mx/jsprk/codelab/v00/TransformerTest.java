package com.bbva.lrba.mx.jsprk.codelab.v00;

import com.bbva.lrba.spark.test.LRBASparkTest;
import com.bbva.lrba.spark.wrapper.DatasetUtils;
import com.bbva.lrba.mx.jsprk.codelab.v00.model.RowData;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class TransformerTest extends LRBASparkTest {
    private Transformer transformer;

    @BeforeEach
    void setUp() {
        this.transformer = new Transformer();
    }

    @Test
    void transform_Output() {
        StructType schema1 = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("ENTIDAD", DataTypes.StringType, false),
                DataTypes.createStructField("DNI", DataTypes.IntegerType, false),
                DataTypes.createStructField("NOMBRE", DataTypes.StringType, false),
                DataTypes.createStructField("TELEFONO", DataTypes.StringType, false),
        });
        Row firstRow1 = RowFactory.create("0182", 1, "John Doe", "123-456");
        Row secondRow1 = RowFactory.create("0182", 2, "Mike Doe", "123-567");
        Row thirdRow1 = RowFactory.create("0182", 3, "Paul Doe", "123-678");
        Row fourRow1 = RowFactory.create("0182", 4, "Rick Doe", "123-785");

        final List<Row> listRows1 = Arrays.asList(firstRow1, secondRow1, thirdRow1, fourRow1);

        StructType schemaNew = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("ENTIDAD", DataTypes.StringType, false),
                DataTypes.createStructField("CP", DataTypes.StringType, false),
        });
        Row firstRowNew = RowFactory.create("0182", "55762");
        Row secondRowNew = RowFactory.create("0182", "55768");
        Row thirdRowNew = RowFactory.create("0182", "55762");
        Row fourRowNew = RowFactory.create("0182", "55762");

        final List<Row> listRowsNew = Arrays.asList(firstRowNew, secondRowNew, thirdRowNew, fourRowNew);

        DatasetUtils<Row> datasetUtils = new DatasetUtils<>();
        Dataset<Row> input1 = datasetUtils.createDataFrame(listRows1, schema1);
        Dataset<Row> input3 = datasetUtils.createDataFrame(listRowsNew, schemaNew);

        final Map<String, Dataset<Row>> datasetMap = this.transformer.transform(new HashMap<>(Map.of("sourceAlias1", input1, "sourceAlias3", input3)));

        assertNotNull(datasetMap);
        assertEquals(1, datasetMap.size());

        Dataset<RowData> returnedDs = datasetMap.get("joinDataset31").as(Encoders.bean(RowData.class));
        System.out.println("Impresion de joinDataset31 en class TransformerTest:");
        returnedDs.show();
        final List<RowData> rows = returnedDs.collectAsList();

        assertEquals(4, rows.size());
        assertEquals("0182", rows.get(0).getENTIDAD());
        assertEquals("John Doe", rows.get(0).getNOMBRE());
        assertEquals("123-456", rows.get(0).getTELEFONO());
        assertEquals("55762", rows.get(0).getCP());
    }
}