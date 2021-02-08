
package org.pentaho.hadoop.shim.capability.generated;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
    "id",
    "shimType",
    "feature"
})
public class ShimCapability {

    @JsonProperty("id")
    private String id;
    @JsonProperty("shimType")
    private String shimType;
    @JsonProperty("feature")
    private Feature feature;
    protected final static Object NOT_FOUND_VALUE = new Object();

    @JsonProperty("id")
    public String getId() {
        return id;
    }

    @JsonProperty("id")
    public void setId(String id) {
        this.id = id;
    }

    @JsonProperty("shimType")
    public String getShimType() {
        return shimType;
    }

    @JsonProperty("shimType")
    public void setShimType(String shimType) {
        this.shimType = shimType;
    }

    @JsonProperty("feature")
    public Feature getFeature() {
        return feature;
    }

    @JsonProperty("feature")
    public void setFeature(Feature feature) {
        this.feature = feature;
    }

    protected boolean declaredProperty(String name, Object value) {
        switch (name) {
            case "id":
                if (value instanceof String) {
                    setId(((String) value));
                } else {
                    throw new IllegalArgumentException(("property \"id\" is of type \"java.lang.String\", but got "+ value.getClass().toString()));
                }
                return true;
            case "shimType":
                if (value instanceof String) {
                    setShimType(((String) value));
                } else {
                    throw new IllegalArgumentException(("property \"shimType\" is of type \"java.lang.String\", but got "+ value.getClass().toString()));
                }
                return true;
            case "feature":
                if (value instanceof Feature) {
                    setFeature(((Feature) value));
                } else {
                    throw new IllegalArgumentException(("property \"feature\" is of type \"org.pentaho.hadoop.shim.capability.generated.Feature\", but got "+ value.getClass().toString()));
                }
                return true;
            default:
                return false;
        }
    }

    protected Object declaredPropertyOrNotFound(String name, Object notFoundValue) {
        switch (name) {
            case "id":
                return getId();
            case "shimType":
                return getShimType();
            case "feature":
                return getFeature();
            default:
                return notFoundValue;
        }
    }

    @SuppressWarnings({
        "unchecked"
    })
    public<T >T get(String name) {
        Object value = declaredPropertyOrNotFound(name, ShimCapability.NOT_FOUND_VALUE);
        if (ShimCapability.NOT_FOUND_VALUE!= value) {
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
        sb.append(ShimCapability.class.getName()).append('@').append(Integer.toHexString(System.identityHashCode(this))).append('[');
        sb.append("id");
        sb.append('=');
        sb.append(((this.id == null)?"<null>":this.id));
        sb.append(',');
        sb.append("shimType");
        sb.append('=');
        sb.append(((this.shimType == null)?"<null>":this.shimType));
        sb.append(',');
        sb.append("feature");
        sb.append('=');
        sb.append(((this.feature == null)?"<null>":this.feature));
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
        result = ((result* 31)+((this.id == null)? 0 :this.id.hashCode()));
        result = ((result* 31)+((this.feature == null)? 0 :this.feature.hashCode()));
        result = ((result* 31)+((this.shimType == null)? 0 :this.shimType.hashCode()));
        return result;
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if ((other instanceof ShimCapability) == false) {
            return false;
        }
        ShimCapability rhs = ((ShimCapability) other);
        return ((((this.id == rhs.id)||((this.id!= null)&&this.id.equals(rhs.id)))&&((this.feature == rhs.feature)||((this.feature!= null)&&this.feature.equals(rhs.feature))))&&((this.shimType == rhs.shimType)||((this.shimType!= null)&&this.shimType.equals(rhs.shimType))));
    }

}
