
package org.pentaho.hadoop.shim.capability.generated;

import java.util.ArrayList;
import java.util.List;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
    "shimCapabilities"
})
public class BigData {

    @JsonProperty("shimCapabilities")
    private List<ShimCapability> shimCapabilities = new ArrayList<ShimCapability>();
    protected final static Object NOT_FOUND_VALUE = new Object();

    @JsonProperty("shimCapabilities")
    public List<ShimCapability> getShimCapabilities() {
        return shimCapabilities;
    }

    @JsonProperty("shimCapabilities")
    public void setShimCapabilities(List<ShimCapability> shimCapabilities) {
        this.shimCapabilities = shimCapabilities;
    }

    protected boolean declaredProperty(String name, Object value) {
        switch (name) {
            case "shimCapabilities":
                if (value instanceof List) {
                    setShimCapabilities(((List<ShimCapability> ) value));
                } else {
                    throw new IllegalArgumentException(("property \"shimCapabilities\" is of type \"java.util.List<org.pentaho.hadoop.shim.capability.generated.ShimCapability>\", but got "+ value.getClass().toString()));
                }
                return true;
            default:
                return false;
        }
    }

    protected Object declaredPropertyOrNotFound(String name, Object notFoundValue) {
        switch (name) {
            case "shimCapabilities":
                return getShimCapabilities();
            default:
                return notFoundValue;
        }
    }

    @SuppressWarnings({
        "unchecked"
    })
    public<T >T get(String name) {
        Object value = declaredPropertyOrNotFound(name, BigData.NOT_FOUND_VALUE);
        if (BigData.NOT_FOUND_VALUE!= value) {
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
        sb.append(BigData.class.getName()).append('@').append(Integer.toHexString(System.identityHashCode(this))).append('[');
        sb.append("shimCapabilities");
        sb.append('=');
        sb.append(((this.shimCapabilities == null)?"<null>":this.shimCapabilities));
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
        result = ((result* 31)+((this.shimCapabilities == null)? 0 :this.shimCapabilities.hashCode()));
        return result;
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if ((other instanceof BigData) == false) {
            return false;
        }
        BigData rhs = ((BigData) other);
        return ((this.shimCapabilities == rhs.shimCapabilities)||((this.shimCapabilities!= null)&&this.shimCapabilities.equals(rhs.shimCapabilities)));
    }

}
