package org.pentaho.hadoop.shim.capability;

import org.pentaho.hadoop.shim.api.internal.capability.ShimCapabilityContext;
import org.pentaho.hadoop.shim.capability.generated.ShimCapability;

public class ShimCapabilityContextImpl implements ShimCapabilityContext {
  ShimCapability shimCapability;

  public ShimCapabilityContextImpl( ShimCapability shimCapability ) {
    this.shimCapability = shimCapability;
  }

  public ShimCapability getShimCapability() {
    return shimCapability;
  }

  @Override
  public boolean equals( Object o ) {
    if (o == this) {
      return true;
    }
    ShimCapabilityContextImpl s = (ShimCapabilityContextImpl) o;
    return this.getShimCapability().equals( s.getShimCapability() );
  }
}
