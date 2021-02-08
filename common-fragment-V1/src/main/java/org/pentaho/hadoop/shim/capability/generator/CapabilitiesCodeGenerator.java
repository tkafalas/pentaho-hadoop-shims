package org.pentaho.hadoop.shim.capability.generator;

import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.sun.codemodel.JCodeModel;
import org.apache.commons.io.FileUtils;
import org.jsonschema2pojo.ContentResolver;
import org.jsonschema2pojo.DefaultGenerationConfig;
import org.jsonschema2pojo.GenerationConfig;
import org.jsonschema2pojo.Jackson2Annotator;
import org.jsonschema2pojo.SchemaGenerator;
import org.jsonschema2pojo.SchemaMapper;
import org.jsonschema2pojo.SchemaStore;
import org.jsonschema2pojo.SourceType;
import org.jsonschema2pojo.rules.RuleFactory;
import org.pentaho.hadoop.shim.api.internal.capability.CapabilitiesProvider;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Paths;

public class CapabilitiesCodeGenerator {
  private static final String JAVA_SOURCE_PATH =
    Paths.get( "" ).toFile().getAbsolutePath() + File.separator + "src" + File.separator + "main" + File.separator
      + "java";
  private static final String targetPackage = "org.pentaho.hadoop.shim.capability.generated";

  public static void main( String[] args ) {
    File removeFolder =
      new File( JAVA_SOURCE_PATH + File.separator + targetPackage.replace( '.', File.separatorChar ) );

    System.out.println( "Removing Folder " + removeFolder.getAbsolutePath() );
    if ( removeFolder.exists() ) {
      try {
        FileUtils.deleteDirectory( removeFolder );
      } catch ( IOException e ) {
        e.printStackTrace();
      }
    }

    JCodeModel codeModel = new JCodeModel();
    URL source =
      Thread.currentThread().getContextClassLoader()
        .getResource( CapabilitiesProvider.DEFAULT_CAPABILITIES_FILE );
    System.out.println( "Using Source File: " + source );

    GenerationConfig config = new DefaultGenerationConfig() {
      @Override
      public SourceType getSourceType() {
        return SourceType.YAML;
      }

      @Override
      public boolean isIncludeAdditionalProperties() {
        return false;
      }

      @Override
      public String getTargetVersion() {
        return "1.8";
      }

      @Override
      public boolean isIncludeDynamicAccessors() {
        return true;
      }

      @Override
      public boolean isIncludeDynamicGetters() {
        return true;
      }

    };

    SchemaMapper mapper =
      new SchemaMapper( new RuleFactory( config, new Jackson2Annotator( config ),
        new SchemaStore( new ContentResolver( new YAMLFactory() ) ) ),
        new SchemaGenerator( new YAMLFactory() ) );
    mapper.generate( codeModel, "Capabilities", "org.pentaho.hadoop.shim.capability.generated", source );

    try {
      //codeModel.build( new File( "C:\\env\\ws\\pentaho-hadoop-shims\\common-fragment-V1\\src\\main\\java" ) );
      codeModel.build( new File( JAVA_SOURCE_PATH ) );
    } catch ( IOException e ) {
      e.printStackTrace();
    }
  }

}
