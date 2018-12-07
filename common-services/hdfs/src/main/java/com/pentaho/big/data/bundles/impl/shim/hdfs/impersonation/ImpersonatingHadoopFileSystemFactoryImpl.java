package com.pentaho.big.data.bundles.impl.shim.hdfs.impersonation;

import com.google.common.base.Throwables;
import com.pentaho.big.data.bundles.impl.shim.hdfs.HadoopFileSystemCallable;
import com.pentaho.big.data.bundles.impl.shim.hdfs.HadoopFileSystemFactoryImpl;
import com.pentaho.big.data.ee.secure.impersonation.api.AuthenticationRequest;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.pentaho.authentication.mapper.api.AuthenticationMappingManager;
import org.pentaho.authentication.mapper.api.MappingException;
import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.bigdata.api.hdfs.HadoopFileSystem;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.hadoop.shim.api.Configuration;
import org.pentaho.hadoop.shim.spi.HadoopShim;
import org.pentaho.hadoop.shim.spi.ShimIdentifierInterface;

import java.io.IOException;
import java.net.URI;
import java.security.PrivilegedAction;
import java.util.Optional;

public class ImpersonatingHadoopFileSystemFactoryImpl extends HadoopFileSystemFactoryImpl {
  private static Class<?> PKG = ImpersonatingHadoopFileSystemFactoryImpl.class;
  // for i18n purposes, needed by Translator2!!

  AuthenticationMappingManager authenticationMappingManager;
  HadoopShim hadoopShim;

  public ImpersonatingHadoopFileSystemFactoryImpl( HadoopShim hadoopShim,
                                                   AuthenticationMappingManager authenticationMappingManager,
                                                   ShimIdentifierInterface shimIdentifier ) {
    this( true, hadoopShim, authenticationMappingManager, "hdfs", shimIdentifier );
  }

  public ImpersonatingHadoopFileSystemFactoryImpl( boolean isActiveConfiguration,
                                                   HadoopShim hadoopShim,
                                                   AuthenticationMappingManager authenticationMappingManager,
                                                   String scheme,
                                                   ShimIdentifierInterface shimIdentifier ) {
    super( isActiveConfiguration, hadoopShim, scheme, shimIdentifier );
    this.authenticationMappingManager = authenticationMappingManager;
    this.hadoopShim = hadoopShim;
  }

  @Override
  public HadoopFileSystem create( NamedCluster namedCluster, URI uri ) throws IOException {
    Optional<UserGroupInformation> ugi = getMapping( namedCluster );
    if ( ugi.isPresent() ) {
      return ugi.get().doAs( (PrivilegedAction<HadoopFileSystem>) () -> {
        try {
          return internalCreate( namedCluster, uri );
        } catch ( IOException e ) {
          throw Throwables.propagate( e );
        }
      } );
    }
    return internalCreate( namedCluster, uri );
  }

  private HadoopFileSystem internalCreate( NamedCluster namedCluster, URI uri ) throws IOException {
    URI tempUri = uri;
    if ( uri == null ) {
      tempUri = URI.create( "" );
    }
    final URI finalUri = tempUri;
    final Configuration configuration = hadoopShim.createConfiguration( namedCluster.getConfigId() );
    FileSystem fileSystem = null;

    fileSystem = (FileSystem) hadoopShim.getFileSystem( configuration ).getDelegate();

    if ( fileSystem instanceof LocalFileSystem ) {
      throw new IOException(
        BaseMessages.getString( PKG, "ImpersonatingHadoopFileSystemFactoryImpl.Error.GotLocalExpectingHDFS" ) );
    }

    return new ImpersonatingHadoopFileSystemImpl( new HadoopFileSystemCallable() {
      @Override
      public FileSystem getFileSystem() {
        try {
          return (FileSystem) hadoopShim.getFileSystem( configuration ).getDelegate();
        } catch ( IOException e ) {
          return null;
        }
      }
    }, authenticationMappingManager, namedCluster );
  }

  private Optional<UserGroupInformation> getMapping( NamedCluster namedCluster ) {
    try {
      return Optional.ofNullable(
        authenticationMappingManager.getMapping(
          AuthenticationRequest.class,
          AuthenticationRequest.create( namedCluster ),
          UserGroupInformation.class
        )
      );
    } catch ( MappingException mappingException ) {
      throw new RuntimeException( mappingException );
    }
  }
}
