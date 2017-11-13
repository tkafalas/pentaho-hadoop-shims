/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2017 by Hitachi Vantara : http://www.pentaho.com
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
package org.pentaho.hadoop.shim.common.format.orc;

import org.apache.orc.TypeDescription;
import org.pentaho.hadoop.shim.api.format.IPentahoOrcOutputFormat;
import org.pentaho.hadoop.shim.api.format.SchemaDescription;

/**
 * Created by tkafalas on 11/3/2017.
 */
public class PentahoOrcOutputFormat implements IPentahoOrcOutputFormat {
  private TypeDescription schema;
  private String outputFilename;
  private SchemaDescription schemaDescription;
  //private CodecFactory codecFactory;

  private String schemaFilename;

  @Override public IPentahoRecordWriter createRecordWriter() throws Exception {
    if ( schemaDescription == null ) {
      throw new Exception( "Invalid state.  The schemaDescription is null" );
    }
    if ( outputFilename == null ) {
      throw new Exception( "Invalid state.  The outputFileName is null" );
    }
    OrcSchemaConverter converter = new OrcSchemaConverter( );
    schema = converter.buildTypeDescription( schemaDescription );

    return new PentahoOrcRecordWriter( schemaDescription, schema, outputFilename );
  }

  @Override public void setSchemaDescription( SchemaDescription schema ) throws Exception {
    schemaDescription = schema;
  }

  @Override public void setOutputFile( String file ) throws Exception {
    outputFilename = file;
  }

  @Override public void setCompression( COMPRESSION compression ) {

  }

  @Override public void setSchemaFilename( String schemaFilename ) {

  }
}
