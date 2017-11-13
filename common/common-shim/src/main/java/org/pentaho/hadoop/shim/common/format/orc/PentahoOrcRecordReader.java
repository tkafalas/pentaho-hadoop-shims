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
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.hadoop.shim.api.format.IPentahoOrcInputFormat;
import org.pentaho.hadoop.shim.api.format.SchemaDescription;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by tkafalas on 11/7/2017.
 */
public class PentahoOrcRecordReader implements IPentahoOrcInputFormat.IPentahoRecordReader {
  private final Configuration conf;
  private final SchemaDescription schemaDescription;
  VectorizedRowBatch batch;
  RecordReader recordReader;
  int currentBatchRow;
  private TypeDescription typeDescription;
  Map<String, Integer> schemaToOrcSubcripts;

  public PentahoOrcRecordReader( String fileName, Configuration conf, SchemaDescription schemaDescription ) {
    this.conf = conf;
    this.schemaDescription = schemaDescription;

    Reader reader = null;
    try {
      reader = OrcFile.createReader( new Path( fileName ),
        OrcFile.readerOptions( conf ) );
    } catch ( IOException e ) {
      throw new RuntimeException( "Unable to read data from file " + fileName, e );
    }
    try {
      recordReader = reader.rows();
    } catch ( IOException e ) {
      throw new RuntimeException( "Unable to get record reader for file " + fileName, e );
    }
    typeDescription = reader.getSchema();
    batch = typeDescription.createRowBatch();

    //Create a map of orc fields to meta columns
    Map<String, Integer> orcColumnNumberMap = new HashMap<String, Integer>();
    int orcFieldNumber = 0;
    for ( String orcFieldName : typeDescription.getFieldNames() ) {
      orcColumnNumberMap.put( orcFieldName, orcFieldNumber++ );
    }

    //Create a map of schemaDescription fields to Orc Column numbers
    schemaToOrcSubcripts = new HashMap<String, Integer>();
    for ( SchemaDescription.Field field : schemaDescription ) {
      if ( field != null ) {
        Integer colNumber = orcColumnNumberMap.get( field.formatFieldName );
        if ( colNumber == null ) {
          throw new RuntimeException(
            "Column " + field.formatFieldName + " does not exist in the ORC file.  Please use the getFields button" );
        } else {
          schemaToOrcSubcripts.put( field.pentahoFieldName, colNumber );
        }
      }
    }

    try {
      setNextBatch();
    } catch ( IOException e ) {
      throw new RuntimeException( "No rows to read in " + fileName, e );
    }
  }


  private boolean setNextBatch() throws IOException {
    currentBatchRow = 0;
    return recordReader.nextBatch( batch );
  }

  @Override public void close() throws IOException {
    recordReader.close();
  }

  @Override public Iterator<RowMetaAndData> iterator() {
    return new Iterator<RowMetaAndData>() {

      @Override public boolean hasNext() {
        if ( currentBatchRow < batch.size ) {
          return true;
        }
        try {
          return setNextBatch();
        } catch ( IOException e ) {
          e.printStackTrace();
          return false;
        }
      }

      @Override public RowMetaAndData next() {

        RowMetaAndData rowMeta =
          OrcConverter.convertFromOrc( batch, currentBatchRow, schemaDescription, typeDescription, schemaToOrcSubcripts );
        currentBatchRow++;
        return rowMeta;
      }
    };
  }
}
