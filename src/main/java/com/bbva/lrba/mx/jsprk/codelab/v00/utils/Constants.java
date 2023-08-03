package com.bbva.lrba.mx.jsprk.codelab.v00.utils;

public enum Constants {

    ALIAS_TABLE_ONE("sourceAlias1"),
    ALIAS_TABLE_TWO("sourceAlias2"),
    ALIAS_TABLE_THREE("sourceAlias3"),

    DNI_COLUMN("DNI"),
    ENTIDAD_COLUMN("ENTIDAD"),
    NOMBRE_COLUMN("NOMBRE"),
    FECHA_COLUMN("FECHA"),
    DNI_CONDITION_COLUMN("DNI_CONDITION")

    ;
    private String value;

    Constants(String string) {
        this.value = string;
    }

    public String getValue() {
        return value;
    }

}
