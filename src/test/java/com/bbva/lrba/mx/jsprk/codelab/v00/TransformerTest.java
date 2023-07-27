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
                DataTypes.createStructField("DNI", DataTypes.StringType, false),
                DataTypes.createStructField("NOMBRE", DataTypes.StringType, false),
                DataTypes.createStructField("TELEFONO", DataTypes.StringType, false),
        });
        Row firstRow1 = RowFactory.create("0182", "000001", "John Doe", "123-456");
        Row secondRow1 = RowFactory.create("0182", "000002", "Mike Doe", "123-567");
        Row thirdRow1 = RowFactory.create("0182", "000003", "Paul Doe", "123-678");

        final List<Row> listRows1 = Arrays.asList(firstRow1, secondRow1, thirdRow1);

        StructType schema2 = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("DNI", DataTypes.StringType, false),
                DataTypes.createStructField("EMAIL", DataTypes.StringType, false),
        });
        Row firstRow2 = RowFactory.create("000001", "johndoe@gmail.com");
        Row secondRow2 = RowFactory.create("000002", "mikedoe@gmail.com");
        Row thirdRow2 = RowFactory.create("000003", "pauldoe@gmail.com");

        final List<Row> listRows2 = Arrays.asList(firstRow2, secondRow2, thirdRow2);

        DatasetUtils<Row> datasetUtils = new DatasetUtils<>();
        Dataset<Row> input1 = datasetUtils.createDataFrame(listRows1, schema1);
        Dataset<Row> input2 = datasetUtils.createDataFrame(listRows2, schema2);

        final Map<String, Dataset<Row>> datasetMap = this.transformer.transform(new HashMap<>(Map.of("sourceAlias1", input1, "sourceAlias2", input2)));

        assertNotNull(datasetMap);
        assertEquals(1, datasetMap.size());

        Dataset<Row> returnedDs = datasetMap.get("joinDNIDataset");
        returnedDs.show();
        final List<Row> rows = returnedDs.collectAsList();

        assertEquals(1, rows.size());
        assertEquals("000001", rows.get(0).getString(0));
        //assertEquals("0182", rows.get(0).getENTIDAD());
        //assertEquals("John Doe", rows.get(0).getNOMBRE());
        //assertEquals("123-456", rows.get(0).getTELEFONO());
        //assertEquals("johndoe@gmail.com", rows.get(0).getEMAIL());
    }
}