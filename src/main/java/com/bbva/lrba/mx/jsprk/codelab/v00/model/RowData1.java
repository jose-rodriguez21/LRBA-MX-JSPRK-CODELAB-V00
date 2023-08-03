package com.bbva.lrba.mx.jsprk.codelab.v00.model;

import java.time.LocalDate;

public class RowData1 {

    private String ENTIDAD;
    private String NOMBRE;
    private String TELEFONO;
    private String EMAIL;
    private LocalDate FECHA;
    private Integer DNI_CONDITION;
    private Integer DNI;


    public String getENTIDAD() {
        return ENTIDAD;
    }

    public void setENTIDAD(String ENTIDAD) {
        this.ENTIDAD = ENTIDAD;
    }

    public String getNOMBRE() {
        return NOMBRE;
    }

    public void setNOMBRE(String NOMBRE) {
        this.NOMBRE = NOMBRE;
    }

    public String getTELEFONO() {
        return TELEFONO;
    }

    public void setTELEFONO(String TELEFONO) {
        this.TELEFONO = TELEFONO;
    }

    public String getEMAIL() {
        return EMAIL;
    }

    public void setEMAIL(String EMAIL) {
        this.EMAIL = EMAIL;
    }

    public LocalDate getFECHA() {
        return FECHA;
    }

    public void setFECHA(LocalDate FECHA) {
        this.FECHA = FECHA;
    }

    public Integer getDNI_CONDITION() {
        return DNI_CONDITION;
    }

    public void setDNI_CONDITION(Integer DNI_CONDITION) {
        this.DNI_CONDITION = DNI_CONDITION;
    }

    public Integer getDNI() {
        return DNI;
    }

    public void setDNI(Integer DNI) {
        this.DNI = DNI;
    }
}
