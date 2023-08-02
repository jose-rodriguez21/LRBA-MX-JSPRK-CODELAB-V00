package com.bbva.lrba.mx.jsprk.codelab.v00;

import com.bbva.lrba.builder.annotation.Builder;
import com.bbva.lrba.builder.spark.RegisterSparkBuilder;
import com.bbva.lrba.builder.spark.domain.SourcesList;
import com.bbva.lrba.builder.spark.domain.TargetsList;
import com.bbva.lrba.spark.domain.datasource.Source;
import com.bbva.lrba.spark.domain.datatarget.Target;
import com.bbva.lrba.spark.domain.transform.TransformConfig;

@Builder
public class JobCodelabBuilder extends RegisterSparkBuilder {

    public static final String SERVICENAME = "local.logicalDataStore.batch";

    @Override
    public SourcesList registerSources() {
        //EXAMPLE WITH A LOCAL SOURCE FILE
        return SourcesList.builder()
                .add(Source.File.Csv.builder()
                        .alias("sourceAlias1")
                        .physicalName("input1.csv")
                        .serviceName(SERVICENAME)
                        .sql("SELECT * FROM sourceAlias1")
                        .header(true)
                        .delimiter(",")
                        .build())
                .add(Source.File.Csv.builder()
                        .alias("sourceAlias2")
                        .physicalName("input2.csv")
                        .serviceName(SERVICENAME)
                        .sql("SELECT * FROM sourceAlias2")
                        .header(true)
                        .delimiter(",")
                        .build())
                .add(Source.File.Csv.builder()
                        .alias("sourceAlias3")
                        .physicalName("input3.csv")
                        .serviceName(SERVICENAME)
                        .sql("SELECT * FROM sourceAlias3")
                        .header(true)
                        .delimiter(",")
                        .build())
                .build();
    }

    @Override
    public TransformConfig registerTransform() {
        //IF YOU WANT TRANSFORM CLASS
        return TransformConfig.TransformClass.builder().transform(new Transformer()).build();
        //IF YOU WANT SQL TRANSFORM
        //return TransformConfig.SqlStatements.builder().addSql("targetAlias1", "SELECT CAMPO1 FROM sourceAlias1").build();
        //IF YOU DO NOT WANT TRANSFORM
        //return null;
    }

    @Override
    public TargetsList registerTargets() {
        //EXAMPLE WITH A LOCAL TARGET FILE
        return TargetsList.builder()
                .add(Target.File.Csv.builder()
                        .alias("joinDataset31")
                        .physicalName("output/output.csv")
                        .serviceName(SERVICENAME)
                        .header(true)
                        .delimiter(",")
                        .build())
                .build();
    }

}