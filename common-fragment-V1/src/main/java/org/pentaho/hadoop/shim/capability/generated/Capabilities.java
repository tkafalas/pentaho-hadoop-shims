
package org.pentaho.hadoop.shim.capability.generated;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
    "bigData"
})
public class Capabilities {

    @JsonProperty("bigData")
    private BigData bigData;
    protected final static Object NOT_FOUND_VALUE = new Object();

    @JsonProperty("bigData")
    public BigData getBigData() {
        return bigData;
    }

    @JsonProperty("bigData")
    public void setBigData(BigData bigData) {
        this.bigData = bigData;
    }

    protected boolean declaredProperty(String name, Object value) {
        switch (name) {
            case "bigData":
                if (value instanceof BigData) {
                    setBigData(((BigData) value));
                } else {
                    throw new IllegalArgumentException(("property \"bigData\" is of type \"org.pentaho.hadoop.shim.capability.generated.BigData\", but got "+ value.getClass().toString()));
                }
                return true;
            default:
                return false;
        }
    }

    protected Object declaredPropertyOrNotFound(String name, Object notFoundValue) {
        switch (name) {
            case "bigData":
                return getBigData();
            default:
                return notFoundValue;
        }
    }

    @SuppressWarnings({
        "unchecked"
    })
    public<T >T get(String name) {
        Object value = declaredPropertyOrNotFound(name, Capabilities.NOT_FOUND_VALUE);
        if (Capabilities.NOT_FOUND_VALUE!= value) {
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
        sb.append(Capabilities.class.getName()).append('@').append(Integer.toHexString(System.identityHashCode(this))).append('[');
        sb.append("bigData");
        sb.append('=');
        sb.append(((this.bigData == null)?"<null>":this.bigData));
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
        result = ((result* 31)+((this.bigData == null)? 0 :this.bigData.hashCode()));
        return result;
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if ((other instanceof Capabilities) == false) {
            return false;
        }
        Capabilities rhs = ((Capabilities) other);
        return ((this.bigData == rhs.bigData)||((this.bigData!= null)&&this.bigData.equals(rhs.bigData)));
    }

}
