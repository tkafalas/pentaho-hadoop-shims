/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2021 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/
package org.pentaho.hadoop.shim.api.internal.capability;

import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.capability.generated.ShimCapability;

public interface CapabilitiesProvider {
  static final String DEFAULT_CAPABILITIES_FILE = "org/pentaho/hadoop/shim/capability/DefaultCapabilities.yaml";

  ShimCapabilityContext getShim( String shimId );

  ShimCapabilityContext getShim( NamedCluster namedCluster );

  /**
   * Returns the boolean value by following the path given. If the field is not defined in the given shim then the value
   * will be returned from the default shim.
   * @param shimCapabilityContext The ShimCapability object of the shim to be examined.
   * @param pathFromShim  The path is a sequence field names in successive maps, (eg.  "feature.hive.enabled")
   * @return The value requested
   * @throws CapabilityProviderException if the path is invalid
   */
  boolean getBoolean( ShimCapabilityContext shimCapabilityContext, String pathFromShim ) throws CapabilityProviderException;

  boolean getBoolean( NamedCluster namedCluster, String pathFromShim ) throws CapabilityProviderException;

  /**
   * Returns the string value by following the path given. If the field is not defined in the given shim then the value
   * will be returned from the default shim.
   * @param shimCapabilityContext The ShimCapability object of the shim to be examined.
   * @param pathFromShim  The path is a sequence field names in successive maps, (eg.  "feature.hive.version")
   * @return The value requested
   * @throws CapabilityProviderException if the path is invalid
   */
  String getValue( ShimCapabilityContext shimCapabilityContext, String pathFromShim ) throws CapabilityProviderException;

  String getValue( NamedCluster namedCluster, String pathFromShim ) throws CapabilityProviderException;

}
