package org.pentaho.hadoop.shim.capability;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.internal.capability.CapabilityProviderException;
import org.pentaho.hadoop.shim.api.internal.capability.ShimCapabilityContext;
import org.pentaho.hadoop.shim.capability.generated.ShimCapability;

import java.io.File;
import java.net.URISyntaxException;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CapabilitiesProviderImplTest {
  private static final String SHIM_ID = "abc12";
  private CapabilitiesProviderImpl provider;
  private ShimCapabilityContext shimCapabilityContext;
  private NamedCluster mockNamedCluster = mock( NamedCluster.class );

  @Before
  public void setUp() throws Exception {
    provider = new CapabilitiesProviderImpl();

    File file = new File(
      Thread.currentThread().getContextClassLoader().getSystemResource( "capability/TestShim.yaml" ).toURI() );
    provider.registerYamlFile( file );
    shimCapabilityContext = provider.getShim( SHIM_ID );
    when( mockNamedCluster.getShimIdentifier() ).thenReturn( SHIM_ID );
  }

  @Test
  public void testGetShim() {
    assertNotNull( shimCapabilityContext );
    assert ( shimCapabilityContext instanceof ShimCapabilityContextImpl );
    ShimCapability shimCapability = ( (ShimCapabilityContextImpl) shimCapabilityContext ).getShimCapability();
    assertEquals( SHIM_ID, shimCapability.getId() );

    assertEquals( shimCapabilityContext, provider.getShim( mockNamedCluster ) );
  }

  @Test
  public void getBoolean() {
    assertTrue( provider.getBoolean( shimCapabilityContext, "feature.hdfs.enabled" ) );
    assertTrue( provider.getBoolean( mockNamedCluster, "feature.hdfs.enabled" ) );
    assertTrue( provider.getBoolean( shimCapabilityContext, "feature.hbase.enabled" ) );
    assertFalse( provider.getBoolean( shimCapabilityContext, "feature.parquet.enabled" ) );

    try {
      provider.getBoolean( shimCapabilityContext, "feature.hdfs" );
      fail( "Should have thrown CapabilityProviderException" );
    } catch ( CapabilityProviderException e ) {
      //expected error
      assert ( e.getMessage().contains( "Cannot cast object to a Boolean" ) );
    }
  }

  @Test
  public void getValue() {
    assertEquals( SHIM_ID, provider.getValue( shimCapabilityContext, "id" ) );
    assertEquals( "1.2", provider.getValue( shimCapabilityContext, "feature.hive.version" ) );
    assertEquals( "1.2", provider.getValue( mockNamedCluster, "feature.hive.version" ) );
  }

  @Test
  public void testGetValue() {
  }
}