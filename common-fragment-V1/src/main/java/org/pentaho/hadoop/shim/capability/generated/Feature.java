
package org.pentaho.hadoop.shim.capability.generated;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
    "hdfs",
    "hive",
    "hbase",
    "sqoop",
    "oozie",
    "kerberos",
    "Knox",
    "Impala",
    "parquet",
    "orc",
    "avro"
})
public class Feature {

    @JsonProperty("hdfs")
    private Hdfs hdfs;
    @JsonProperty("hive")
    private Hive hive;
    @JsonProperty("hbase")
    private Hbase hbase;
    @JsonProperty("sqoop")
    private Sqoop sqoop;
    @JsonProperty("oozie")
    private Oozie oozie;
    @JsonProperty("kerberos")
    private Kerberos kerberos;
    @JsonProperty("Knox")
    private Knox knox;
    @JsonProperty("Impala")
    private Impala impala;
    @JsonProperty("parquet")
    private Parquet parquet;
    @JsonProperty("orc")
    private Orc orc;
    @JsonProperty("avro")
    private Avro avro;
    protected final static Object NOT_FOUND_VALUE = new Object();

    @JsonProperty("hdfs")
    public Hdfs getHdfs() {
        return hdfs;
    }

    @JsonProperty("hdfs")
    public void setHdfs(Hdfs hdfs) {
        this.hdfs = hdfs;
    }

    @JsonProperty("hive")
    public Hive getHive() {
        return hive;
    }

    @JsonProperty("hive")
    public void setHive(Hive hive) {
        this.hive = hive;
    }

    @JsonProperty("hbase")
    public Hbase getHbase() {
        return hbase;
    }

    @JsonProperty("hbase")
    public void setHbase(Hbase hbase) {
        this.hbase = hbase;
    }

    @JsonProperty("sqoop")
    public Sqoop getSqoop() {
        return sqoop;
    }

    @JsonProperty("sqoop")
    public void setSqoop(Sqoop sqoop) {
        this.sqoop = sqoop;
    }

    @JsonProperty("oozie")
    public Oozie getOozie() {
        return oozie;
    }

    @JsonProperty("oozie")
    public void setOozie(Oozie oozie) {
        this.oozie = oozie;
    }

    @JsonProperty("kerberos")
    public Kerberos getKerberos() {
        return kerberos;
    }

    @JsonProperty("kerberos")
    public void setKerberos(Kerberos kerberos) {
        this.kerberos = kerberos;
    }

    @JsonProperty("Knox")
    public Knox getKnox() {
        return knox;
    }

    @JsonProperty("Knox")
    public void setKnox(Knox knox) {
        this.knox = knox;
    }

    @JsonProperty("Impala")
    public Impala getImpala() {
        return impala;
    }

    @JsonProperty("Impala")
    public void setImpala(Impala impala) {
        this.impala = impala;
    }

    @JsonProperty("parquet")
    public Parquet getParquet() {
        return parquet;
    }

    @JsonProperty("parquet")
    public void setParquet(Parquet parquet) {
        this.parquet = parquet;
    }

    @JsonProperty("orc")
    public Orc getOrc() {
        return orc;
    }

    @JsonProperty("orc")
    public void setOrc(Orc orc) {
        this.orc = orc;
    }

    @JsonProperty("avro")
    public Avro getAvro() {
        return avro;
    }

    @JsonProperty("avro")
    public void setAvro(Avro avro) {
        this.avro = avro;
    }

    protected boolean declaredProperty(String name, Object value) {
        switch (name) {
            case "hdfs":
                if (value instanceof Hdfs) {
                    setHdfs(((Hdfs) value));
                } else {
                    throw new IllegalArgumentException(("property \"hdfs\" is of type \"org.pentaho.hadoop.shim.capability.generated.Hdfs\", but got "+ value.getClass().toString()));
                }
                return true;
            case "hive":
                if (value instanceof Hive) {
                    setHive(((Hive) value));
                } else {
                    throw new IllegalArgumentException(("property \"hive\" is of type \"org.pentaho.hadoop.shim.capability.generated.Hive\", but got "+ value.getClass().toString()));
                }
                return true;
            case "hbase":
                if (value instanceof Hbase) {
                    setHbase(((Hbase) value));
                } else {
                    throw new IllegalArgumentException(("property \"hbase\" is of type \"org.pentaho.hadoop.shim.capability.generated.Hbase\", but got "+ value.getClass().toString()));
                }
                return true;
            case "sqoop":
                if (value instanceof Sqoop) {
                    setSqoop(((Sqoop) value));
                } else {
                    throw new IllegalArgumentException(("property \"sqoop\" is of type \"org.pentaho.hadoop.shim.capability.generated.Sqoop\", but got "+ value.getClass().toString()));
                }
                return true;
            case "oozie":
                if (value instanceof Oozie) {
                    setOozie(((Oozie) value));
                } else {
                    throw new IllegalArgumentException(("property \"oozie\" is of type \"org.pentaho.hadoop.shim.capability.generated.Oozie\", but got "+ value.getClass().toString()));
                }
                return true;
            case "kerberos":
                if (value instanceof Kerberos) {
                    setKerberos(((Kerberos) value));
                } else {
                    throw new IllegalArgumentException(("property \"kerberos\" is of type \"org.pentaho.hadoop.shim.capability.generated.Kerberos\", but got "+ value.getClass().toString()));
                }
                return true;
            case "Knox":
                if (value instanceof Knox) {
                    setKnox(((Knox) value));
                } else {
                    throw new IllegalArgumentException(("property \"Knox\" is of type \"org.pentaho.hadoop.shim.capability.generated.Knox\", but got "+ value.getClass().toString()));
                }
                return true;
            case "Impala":
                if (value instanceof Impala) {
                    setImpala(((Impala) value));
                } else {
                    throw new IllegalArgumentException(("property \"Impala\" is of type \"org.pentaho.hadoop.shim.capability.generated.Impala\", but got "+ value.getClass().toString()));
                }
                return true;
            case "parquet":
                if (value instanceof Parquet) {
                    setParquet(((Parquet) value));
                } else {
                    throw new IllegalArgumentException(("property \"parquet\" is of type \"org.pentaho.hadoop.shim.capability.generated.Parquet\", but got "+ value.getClass().toString()));
                }
                return true;
            case "orc":
                if (value instanceof Orc) {
                    setOrc(((Orc) value));
                } else {
                    throw new IllegalArgumentException(("property \"orc\" is of type \"org.pentaho.hadoop.shim.capability.generated.Orc\", but got "+ value.getClass().toString()));
                }
                return true;
            case "avro":
                if (value instanceof Avro) {
                    setAvro(((Avro) value));
                } else {
                    throw new IllegalArgumentException(("property \"avro\" is of type \"org.pentaho.hadoop.shim.capability.generated.Avro\", but got "+ value.getClass().toString()));
                }
                return true;
            default:
                return false;
        }
    }

    protected Object declaredPropertyOrNotFound(String name, Object notFoundValue) {
        switch (name) {
            case "hdfs":
                return getHdfs();
            case "hive":
                return getHive();
            case "hbase":
                return getHbase();
            case "sqoop":
                return getSqoop();
            case "oozie":
                return getOozie();
            case "kerberos":
                return getKerberos();
            case "Knox":
                return getKnox();
            case "Impala":
                return getImpala();
            case "parquet":
                return getParquet();
            case "orc":
                return getOrc();
            case "avro":
                return getAvro();
            default:
                return notFoundValue;
        }
    }

    @SuppressWarnings({
        "unchecked"
    })
    public<T >T get(String name) {
        Object value = declaredPropertyOrNotFound(name, Feature.NOT_FOUND_VALUE);
        if (Feature.NOT_FOUND_VALUE!= value) {
            return ((T) value);
        } else {
            throw new IllegalArgumentException((("property \""+ name)+"\" is not defined"));
        }
    }

    public void set(String name, Object value) {
        if (!declaredProperty(name, value)) {
            throw new IllegalArgumentException((("property \""+ name)+"\" is not defined"));
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(Feature.class.getName()).append('@').append(Integer.toHexString(System.identityHashCode(this))).append('[');
        sb.append("hdfs");
        sb.append('=');
        sb.append(((this.hdfs == null)?"<null>":this.hdfs));
        sb.append(',');
        sb.append("hive");
        sb.append('=');
        sb.append(((this.hive == null)?"<null>":this.hive));
        sb.append(',');
        sb.append("hbase");
        sb.append('=');
        sb.append(((this.hbase == null)?"<null>":this.hbase));
        sb.append(',');
        sb.append("sqoop");
        sb.append('=');
        sb.append(((this.sqoop == null)?"<null>":this.sqoop));
        sb.append(',');
        sb.append("oozie");
        sb.append('=');
        sb.append(((this.oozie == null)?"<null>":this.oozie));
        sb.append(',');
        sb.append("kerberos");
        sb.append('=');
        sb.append(((this.kerberos == null)?"<null>":this.kerberos));
        sb.append(',');
        sb.append("knox");
        sb.append('=');
        sb.append(((this.knox == null)?"<null>":this.knox));
        sb.append(',');
        sb.append("impala");
        sb.append('=');
        sb.append(((this.impala == null)?"<null>":this.impala));
        sb.append(',');
        sb.append("parquet");
        sb.append('=');
        sb.append(((this.parquet == null)?"<null>":this.parquet));
        sb.append(',');
        sb.append("orc");
        sb.append('=');
        sb.append(((this.orc == null)?"<null>":this.orc));
        sb.append(',');
        sb.append("avro");
        sb.append('=');
        sb.append(((this.avro == null)?"<null>":this.avro));
        sb.append(',');
        if (sb.charAt((sb.length()- 1)) == ',') {
            sb.setCharAt((sb.length()- 1), ']');
        } else {
            sb.append(']');
        }
        return sb.toString();
    }

    @Override
    public int hashCode() {
        int result = 1;
        result = ((result* 31)+((this.hive == null)? 0 :this.hive.hashCode()));
        result = ((result* 31)+((this.orc == null)? 0 :this.orc.hashCode()));
        result = ((result* 31)+((this.oozie == null)? 0 :this.oozie.hashCode()));
        result = ((result* 31)+((this.kerberos == null)? 0 :this.kerberos.hashCode()));
        result = ((result* 31)+((this.impala == null)? 0 :this.impala.hashCode()));
        result = ((result* 31)+((this.parquet == null)? 0 :this.parquet.hashCode()));
        result = ((result* 31)+((this.hdfs == null)? 0 :this.hdfs.hashCode()));
        result = ((result* 31)+((this.sqoop == null)? 0 :this.sqoop.hashCode()));
        result = ((result* 31)+((this.hbase == null)? 0 :this.hbase.hashCode()));
        result = ((result* 31)+((this.knox == null)? 0 :this.knox.hashCode()));
        result = ((result* 31)+((this.avro == null)? 0 :this.avro.hashCode()));
        return result;
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if ((other instanceof Feature) == false) {
            return false;
        }
        Feature rhs = ((Feature) other);
        return ((((((((((((this.hive == rhs.hive)||((this.hive!= null)&&this.hive.equals(rhs.hive)))&&((this.orc == rhs.orc)||((this.orc!= null)&&this.orc.equals(rhs.orc))))&&((this.oozie == rhs.oozie)||((this.oozie!= null)&&this.oozie.equals(rhs.oozie))))&&((this.kerberos == rhs.kerberos)||((this.kerberos!= null)&&this.kerberos.equals(rhs.kerberos))))&&((this.impala == rhs.impala)||((this.impala!= null)&&this.impala.equals(rhs.impala))))&&((this.parquet == rhs.parquet)||((this.parquet!= null)&&this.parquet.equals(rhs.parquet))))&&((this.hdfs == rhs.hdfs)||((this.hdfs!= null)&&this.hdfs.equals(rhs.hdfs))))&&((this.sqoop == rhs.sqoop)||((this.sqoop!= null)&&this.sqoop.equals(rhs.sqoop))))&&((this.hbase == rhs.hbase)||((this.hbase!= null)&&this.hbase.equals(rhs.hbase))))&&((this.knox == rhs.knox)||((this.knox!= null)&&this.knox.equals(rhs.knox))))&&((this.avro == rhs.avro)||((this.avro!= null)&&this.avro.equals(rhs.avro))));
    }

}
