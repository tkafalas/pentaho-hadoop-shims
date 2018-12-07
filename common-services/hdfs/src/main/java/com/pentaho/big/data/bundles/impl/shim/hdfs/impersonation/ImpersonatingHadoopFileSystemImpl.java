/*!
 * HITACHI VANTARA PROPRIETARY AND CONFIDENTIAL
 *
 * Copyright 2002 - 2017 Hitachi Vantara. All rights reserved.
 *
 * NOTICE: All information including source code contained herein is, and
 * remains the sole property of Hitachi Vantara and its licensors. The intellectual
 * and technical concepts contained herein are proprietary and confidential
 * to, and are trade secrets of Hitachi Vantara and may be covered by U.S. and foreign
 * patents, or patents in process, and are protected by trade secret and
 * copyright laws. The receipt or possession of this source code and/or related
 * information does not convey or imply any rights to reproduce, disclose or
 * distribute its contents, or to manufacture, use, or sell anything that it
 * may describe, in whole or in part. Any reproduction, modification, distribution,
 * or public display of this information without the express written authorization
 * from Hitachi Vantara is strictly prohibited and in violation of applicable laws and
 * international treaties. Access to the source code contained herein is strictly
 * prohibited to anyone except those individuals and entities who have executed
 * confidentiality and non-disclosure agreements or other agreements with Hitachi Vantara,
 * explicitly covering such access.
 */

package com.pentaho.big.data.bundles.impl.shim.hdfs.impersonation;

import com.pentaho.big.data.bundles.impl.shim.hdfs.HadoopFileSystemCallable;
import com.pentaho.big.data.bundles.impl.shim.hdfs.HadoopFileSystemImpl;
import com.pentaho.big.data.ee.secure.impersonation.api.AuthenticationRequest;
import org.apache.hadoop.security.UserGroupInformation;
import org.pentaho.authentication.mapper.api.AuthenticationMappingManager;
import org.pentaho.authentication.mapper.api.MappingException;
import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.bigdata.api.hdfs.HadoopFileStatus;
import org.pentaho.bigdata.api.hdfs.HadoopFileSystemPath;
import org.pentaho.di.i18n.BaseMessages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.PrivilegedExceptionAction;
import java.util.Optional;


public class ImpersonatingHadoopFileSystemImpl extends HadoopFileSystemImpl {
  private static Class<?> PKG = ImpersonatingHadoopFileSystemImpl.class; // for i18n purposes, needed by Translator2!!

  protected static Logger logger =
    LoggerFactory.getLogger( ImpersonatingHadoopFileSystemImpl.class );
  private final AuthenticationMappingManager authMappingManager;
  private final NamedCluster namedCluster;

  public ImpersonatingHadoopFileSystemImpl( HadoopFileSystemCallable hadoopFileSystemCallable,
                                            AuthenticationMappingManager authMappingManager,
                                            NamedCluster namedCluster ) {
    super( hadoopFileSystemCallable );
    this.authMappingManager = authMappingManager;
    this.namedCluster = namedCluster;
  }

  @Override
  public OutputStream append( HadoopFileSystemPath path ) throws IOException {
    return (OutputStream) wrapAction( "append", path.getPath(), new Object[] { path }, HadoopFileSystemPath.class );
  }

  @Override
  public OutputStream create( HadoopFileSystemPath path ) throws IOException {
    return (OutputStream) wrapAction( "create", path.getPath(), new Object[] { path }, HadoopFileSystemPath.class );
  }

  @Override
  public boolean delete( HadoopFileSystemPath path, boolean arg1 ) throws IOException {
    return (boolean) wrapAction( "delete", path.getPath(), new Object[] { path, arg1 }, HadoopFileSystemPath.class,
      Boolean.TYPE );
  }

  @Override
  public HadoopFileStatus getFileStatus( HadoopFileSystemPath path ) throws IOException {
    return (HadoopFileStatus) wrapAction( "getFileStatus", path.getPath(), new Object[] { path },
      HadoopFileSystemPath.class );
  }

  @Override
  public boolean mkdirs( HadoopFileSystemPath path ) throws IOException {
    return (boolean) wrapAction( "mkdirs", path.getPath(), new Object[] { path }, HadoopFileSystemPath.class );
  }

  @Override
  public InputStream open( HadoopFileSystemPath path ) throws IOException {
    return (InputStream) wrapAction( "open", path.getPath(), new Object[] { path }, HadoopFileSystemPath.class );
  }

  @Override
  public boolean rename( HadoopFileSystemPath path, HadoopFileSystemPath path2 ) throws IOException {
    return (boolean) wrapAction( "rename", path.getPath() + " to " + path2.getPath(), new Object[] { path, path2 },
      HadoopFileSystemPath.class, HadoopFileSystemPath.class );
  }

  @Override
  public void setTimes( HadoopFileSystemPath path, long mtime, long atime ) throws IOException {
    wrapAction( "setTimes", path.getPath(), new Object[] { path, mtime, atime }, HadoopFileSystemPath.class, Long.TYPE,
      Long.TYPE );
  }

  @Override
  public HadoopFileStatus[] listStatus( HadoopFileSystemPath path ) throws IOException {
    return (HadoopFileStatus[]) wrapAction( "listStatus", path.getPath(), new Object[] { path },
      HadoopFileSystemPath.class );
  }

  @Override
  public void chmod( HadoopFileSystemPath path, int permissions ) throws IOException {
    wrapAction( "chmod", path.getPath(), new Object[] { path, permissions }, HadoopFileSystemPath.class, Integer.TYPE );
  }

  @Override
  public boolean exists( HadoopFileSystemPath path ) throws IOException {
    return (boolean) wrapAction( "exists", path.getPath(), new Object[] { path }, HadoopFileSystemPath.class );
  }

  @Override
  public HadoopFileSystemPath resolvePath( HadoopFileSystemPath path ) throws IOException {
    return (HadoopFileSystemPath) wrapAction( "resolvePath", path.getPath(), new Object[] { path },
      HadoopFileSystemPath.class );
  }

  @Override
  public HadoopFileSystemPath getPath( String path ) {
    return (HadoopFileSystemPath) wrapThrowableAction( "getPath", path, new Object[] { path }, String.class );
  }

  @Override
  public HadoopFileSystemPath getHomeDirectory() {
    return (HadoopFileSystemPath) wrapThrowableAction( "getHomeDirectory", "", new Object[] {} );
  }

  @Override
  public HadoopFileSystemPath makeQualified( HadoopFileSystemPath path ) {
    return (HadoopFileSystemPath) wrapThrowableAction( "makeQualified", path.getPath(), new Object[] { path },
      HadoopFileSystemPath.class );
  }

  @Override
  public String getFsDefaultName() {
    return (String) wrapThrowableAction( "getFsDefaultName", "", new Object[] {} );
  }

  @Override
  public void setProperty( String name, String value ) {
    wrapThrowableAction( "setProperty", name, new Object[] { name, value }, String.class, String.class );
  }

  @Override
  public String getProperty( String name, String defaultValue ) {
    return (String) wrapThrowableAction( "getProperty", name, new Object[] { name, defaultValue }, String.class,
      String.class );
  }

  private Object wrapThrowableAction( String methodName, String operationText, Object[] args,
                                      Class<?>... parameterTypes ) {
    try {
      return wrapAction( methodName, operationText, args, parameterTypes );
    } catch ( IOException e ) {
      return null;
    }
  }

  private Object wrapAction( String methodName, String operationText, Object[] args, Class<?>... parameterTypes )
    throws IOException {
    Optional<UserGroupInformation> ugi = getMapping( namedCluster );
    if ( ugi.isPresent() ) {
      UserGroupInformation ugiToUse = ugi.get();
      try {
        logger.debug( ugiToUse.toString() );
        logger.info( BaseMessages.getString( PKG,
          operationText.isEmpty() ? "ImpersonatingHadoopFileSystemImpl.Log.PerformingOperationWithMethod"
            : "ImpersonatingHadoopFileSystemImpl.Log.PerformingOperationWithMethodOperationText", methodName,
          operationText ) );
        return ugiToUse
          .doAs( (PrivilegedExceptionAction<Object>) () -> performAction( methodName, args, parameterTypes ) );
      } catch ( Exception th ) {
        if ( th instanceof IOException ) {
          //rethrow native Hadoop File System exception,
          //as for BACKLOG-9020 - getFileStatus throw FileNotFoundException when file doesn't exist,
          //code expects such exception not InvocationTargetException which is thrown due to reflection call
          throw (IOException) th;
        }
        throw new IOException( th );
      }
    } else {
      return performAction( methodName, args, parameterTypes );
    }
  }

  private Object performAction( String methodName, Object[] args, Class<?>... parameterTypes ) throws IOException {
    try {
      Method method = HadoopFileSystemImpl.class.getMethod( methodName, parameterTypes );
      Thread currentThread = Thread.currentThread();
      ClassLoader contextClassLoader = currentThread.getContextClassLoader();
      try {
        currentThread.setContextClassLoader( ImpersonatingHadoopFileSystemImpl.class.getClassLoader() );
        return method.invoke( new HadoopFileSystemImpl( this.hadoopFileSystemCallable ), args );
      } finally {
        currentThread.setContextClassLoader( contextClassLoader );
      }
    } catch ( Exception th ) {
      if ( th instanceof InvocationTargetException && th.getCause() != null && th.getCause() instanceof IOException ) {
        //rethrow native Hadoop File System exception,
        //as for BACKLOG-9020 - getFileStatus throw FileNotFoundException when file doesn't exist,
        //code expects such exception not InvocationTargetException which is thrown due to reflection call
        throw (IOException) th.getCause();
      }
      throw new IOException( th );
    }

  }

  private Optional<UserGroupInformation> getMapping( NamedCluster namedCluster ) {
    try {
      return Optional.ofNullable(
        authMappingManager.getMapping(
          AuthenticationRequest.class,
          AuthenticationRequest.create( namedCluster ),
          UserGroupInformation.class
        )
      );
    } catch ( MappingException mappingException ) {
      logger.error( mappingException.getMessage() );
      throw new RuntimeException( mappingException );
    }
  }
}

