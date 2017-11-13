/*******************************************************************************
 *
 * Pentaho Big Data
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.pentaho.hadoop.shim.api.format.IPentahoOrcInputFormat;
import org.pentaho.hadoop.shim.api.format.SchemaDescription;

import java.util.List;

/**
 * Created by tkafalas on 11/7/2017.
 */
public class PentahoOrcInputFormat implements IPentahoOrcInputFormat {

  private String fileName;
  private String schemaFileName;
  private SchemaDescription schemaDescription;
  private Configuration conf;

  @Override
  public List<IPentahoInputSplit> getSplits() throws Exception {
    return null;
  }

  @Override
  public IPentahoRecordReader createRecordReader( IPentahoInputSplit split ) throws Exception {
    if ( fileName == null || schemaDescription == null ) {
      throw new IllegalStateException( "fileName or schemaDescription must no be null" );
    }
    conf = new Configuration();
    return new PentahoOrcRecordReader( fileName, conf, schemaDescription );
  }

  @Override
  public SchemaDescription readSchema( String schemaFileName, String fileName ) throws Exception {

    return null;
  }

  /**
   * Set schema from user's metadata
   * <p>
   * This schema will be used instead of schema from {@link #schemaFileName} since we allow user to override pentaho
   * filed name
   */
  @Override
  public void setSchema( SchemaDescription schema ) throws Exception {
    schemaDescription = schema;
  }

  @Override
  public void setInputFile( String fileName ) throws Exception {
    this.fileName = fileName;
  }

  @Override
  public void setSplitSize( long blockSize ) throws Exception {
    //do nothing 
  }

}
