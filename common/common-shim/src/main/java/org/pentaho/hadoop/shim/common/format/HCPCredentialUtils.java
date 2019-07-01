package org.pentaho.hadoop.shim.common.format;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.net.URI;

/**
 * Created by timkafalas on 6/14/2019.
 */
public class HCPCredentialUtils {
  private static final String HCPSCHEME = "hcp";
  private static final String HCPROOT = HCPSCHEME + "/";
  private static final String HCP_FS_CLASS_NAME = "com.pentaho.hcp.vfs.HCPFileSystem";

  public static void applyHCPCredentialsToHadoopConfigurationIfNecessary( String filename, Configuration conf ) {
    Path outputFile = new Path( scrubFilePathIfNecessary( filename ) );
    URI uri = outputFile.toUri();
    String scheme = uri != null ? uri.getScheme() : null;
    if ( scheme != null && scheme.equals( HCPSCHEME ) ) {
      conf.set( "fs.hcp.impl", HCP_FS_CLASS_NAME );
    }
  }

  public static String scrubFilePathIfNecessary( String filename ) {
    return filename != null ? filename.replace( HCPROOT, "" ) : filename;
  }
}
