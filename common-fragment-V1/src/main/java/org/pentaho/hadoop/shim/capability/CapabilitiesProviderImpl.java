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
package org.pentaho.hadoop.shim.capability;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.internal.capability.CapabilitiesProvider;
import org.pentaho.hadoop.shim.api.internal.capability.CapabilityProviderException;
import org.pentaho.hadoop.shim.api.internal.capability.ShimCapabilityContext;
import org.pentaho.hadoop.shim.capability.generated.Capabilities;
import org.pentaho.hadoop.shim.capability.generated.ShimCapability;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class CapabilitiesProviderImpl implements CapabilitiesProvider {
  private ObjectMapper mapper;
  private Capabilities capabilities;
  private ShimCapability defaultShimCapability;

  public CapabilitiesProviderImpl() {
    init();
  }

  public void init() {
    try {
      mapper = new ObjectMapper( new YAMLFactory() );

      //Default capabilities file must exist
      capabilities = deserializeYamlFile(
        Thread.currentThread().getContextClassLoader().getResourceAsStream( DEFAULT_CAPABILITIES_FILE ) );
      //There better be a default shim to hold default values
      try {
        defaultShimCapability = getShimInternal( "default" );
      } catch ( NullPointerException e ) {
        throwStructureException();
      }
      if ( defaultShimCapability == null ) {
        throwStructureException();
      }
    } catch ( IOException e ) {
      throw new CapabilityProviderException( "Could not deserialize Capability Yaml file " + DEFAULT_CAPABILITIES_FILE,
        e );
    }
  }

  public void registerYamlFile( File yamlFile ) throws CapabilityProviderException {
    Capabilities partCapabilites;
    try {
      partCapabilites = deserializeYamlFile( new FileInputStream( yamlFile ) );
    } catch ( IOException e ) {
      throw new CapabilityProviderException( "Could not deserialize Capability Yaml file " + yamlFile.getAbsolutePath(),
        e );
    }

    for ( ShimCapability shimCapability : partCapabilites.getBigData().getShimCapabilities() ) {
      capabilities.getBigData().getShimCapabilities().add( shimCapability );
    }
  }

  private Capabilities deserializeYamlFile( InputStream inputStream ) throws IOException {
    return mapper.readValue( inputStream, Capabilities.class );
  }


  @Override public ShimCapabilityContext getShim( NamedCluster namedCluster ) {
    return new ShimCapabilityContextImpl( getShimInternal( namedCluster.getShimIdentifier() ) );
  }

  @Override public ShimCapabilityContext getShim( String shimId ) {
    return new ShimCapabilityContextImpl( getShimInternal( shimId ) );
  }

  private ShimCapability getShimInternal( String shimId ) {
    assert shimId != null;
    for ( ShimCapability shimCapability : capabilities.getBigData().getShimCapabilities() ) {
      if ( shimId.equals( shimCapability.getId() ) ) {
        return shimCapability;
      }
    }
    return null;
  }

  @Override public String getValue( NamedCluster namedCluster, String pathFromShim ) {
    return getValue( getShim( namedCluster.getShimIdentifier() ), pathFromShim );
  }

  @Override public String getValue( ShimCapabilityContext shimCapabilityContext, String pathFromShim ) {

    Object resultObject = getShimCapabilityObjectByPath( getShimCapability( shimCapabilityContext ), pathFromShim );
    return (String) resultObject;
  }

  @Override public boolean getBoolean( NamedCluster namedCluster, String pathFromShim ) {
    return getBoolean( getShim( namedCluster.getShimIdentifier() ), pathFromShim );
  }

  @Override public boolean getBoolean( ShimCapabilityContext shimCapabilityContext, String pathFromShim )
    throws CapabilityProviderException {
    Object resultObject = getShimCapabilityObjectByPath( getShimCapability( shimCapabilityContext), pathFromShim );
    if ( resultObject != null && !( resultObject instanceof Boolean ) ) {
      throw new CapabilityProviderException(
        "Cannot cast object to a Boolean.  Check path \"" + pathFromShim + "\"" );
    }
    return resultObject instanceof Boolean ? (Boolean) resultObject : false;
  }

  private Object getShimCapabilityObjectByPath( ShimCapability shimCapability, String pathFromShim )
    throws CapabilityProviderException {
    Object resultObject = getObjectByPath( shimCapability, pathFromShim );
    if ( resultObject == null ) {
      resultObject = getObjectByPath( defaultShimCapability, pathFromShim );
    }
    return resultObject;
  }

  private Object getObjectByPath( Object targetObject, String pathFromShim ) {
    for ( String name : pathFromShim.split( "\\." ) ) {
      targetObject = getObjectByName( targetObject, name );
      if ( targetObject == null ) {
        break;
      }
    }
    return targetObject;
  }

  private Object getObjectByName( Object object, String name ) {
    Method method;
    try {
      method = object.getClass().getMethod( "get", String.class );
      return method.invoke( object, name );
    } catch ( SecurityException | IllegalAccessException e ) {
      throw new CapabilityProviderException( "Could not get \" + name + \"", e );
    } catch ( NoSuchMethodException | InvocationTargetException e ) {
      throw new CapabilityProviderException( "No Dynamic accessor method for \" + name + \"", e );
    }
  }

  private void throwStructureException() {
    throw new CapabilityProviderException( "File " + DEFAULT_CAPABILITIES_FILE + " must contain the following"
      + " structure from the root:\n"
      + "bigData:\n"
      + "  shimCapabilities:\n"
      + "    - id: default" );
  }

  private ShimCapability getShimCapability( ShimCapabilityContext shimCapabilityContext ) {
    return ( (ShimCapabilityContextImpl) shimCapabilityContext ).getShimCapability();
  }

}
