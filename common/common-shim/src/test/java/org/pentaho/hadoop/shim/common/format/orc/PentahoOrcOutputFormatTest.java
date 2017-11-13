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

import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaBigNumber;
import org.pentaho.di.core.row.value.ValueMetaBinary;
import org.pentaho.di.core.row.value.ValueMetaBoolean;
import org.pentaho.di.core.row.value.ValueMetaDate;
import org.pentaho.di.core.row.value.ValueMetaInteger;
import org.pentaho.di.core.row.value.ValueMetaInternetAddress;
import org.pentaho.di.core.row.value.ValueMetaNumber;
import org.pentaho.di.core.row.value.ValueMetaPluginType;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.core.row.value.ValueMetaTimestamp;
import org.pentaho.di.core.util.Assert;
import org.pentaho.hadoop.shim.api.format.IPentahoInputFormat;
import org.pentaho.hadoop.shim.api.format.IPentahoOrcOutputFormat;
import org.pentaho.hadoop.shim.api.format.IPentahoOutputFormat;
import org.pentaho.hadoop.shim.api.format.SchemaDescription;

import java.io.File;
import java.math.BigDecimal;
import java.math.MathContext;
import java.net.InetAddress;
import java.sql.Timestamp;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;


/**
 * Created by tkafalas on 11/3/2017.
 */
public class PentahoOrcOutputFormatTest {
  SchemaDescription schemaDescription;
  RowMeta rowMeta;
  Object[][] rowData;
  PentahoOrcOutputFormat orcOutputFormat;
  String filePath;

  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Before
  public void setup() throws Exception {
    tempFolder.create();
    orcOutputFormat = new PentahoOrcOutputFormat();
    orcOutputFormat.setCompression( IPentahoOrcOutputFormat.COMPRESSION.UNCOMPRESSED );
    //orcOutputFormat.setOutputFile( tempFolder.getRoot().toString().substring( 2 ) + "/orcOutput" );
    filePath = "/tmp/orcOutput.orc";
    try {
      orcOutputFormat.setOutputFile( filePath );
    } catch ( Exception e ) {
      e.printStackTrace();
    }
    ( new File( filePath ) ).delete();

    // Set up the Orc Schema Description and add fields to it.  Then set that schemaDescription on the orc output
    // format.
    schemaDescription = new SchemaDescription();
    schemaDescription
      .addField( schemaDescription.new Field( "orcField1", "pentahoField1", ValueMetaInterface.TYPE_STRING, true ) );
    schemaDescription
      .addField( schemaDescription.new Field( "orcField2", "pentahoField2", ValueMetaInterface.TYPE_STRING, true ) );
    schemaDescription
      .addField( schemaDescription.new Field( "orcDouble3", "pentahoNumber3", ValueMetaInterface.TYPE_NUMBER, true ) );
    schemaDescription
      .addField(
        schemaDescription.new Field( "orcDouble4", "pentahoBigNumber4", ValueMetaInterface.TYPE_BIGNUMBER, true ) );
    schemaDescription
      .addField( schemaDescription.new Field( "orcBytes5", "pentahoInet5", ValueMetaInterface.TYPE_INET, true ) );
    schemaDescription
      .addField( schemaDescription.new Field( "orcLong6", "pentahoBoolean6", ValueMetaInterface.TYPE_BOOLEAN, true ) );
    schemaDescription
      .addField( schemaDescription.new Field( "orcInt7", "pentahoInt7", ValueMetaInterface.TYPE_INTEGER, true ) );
    schemaDescription
      .addField( schemaDescription.new Field( "orcDate8", "pentahoDate8", ValueMetaInterface.TYPE_DATE, true ) );
    schemaDescription
      .addField(
        schemaDescription.new Field( "orcTimestamp9", "pentahoTimestamp9", ValueMetaInterface.TYPE_TIMESTAMP, true ) );
    schemaDescription
      .addField( schemaDescription.new Field( "orcBytes10", "pentahoBinary10", ValueMetaInterface.TYPE_BINARY, true ) );
    orcOutputFormat.setSchemaDescription( schemaDescription );

    rowMeta = new RowMeta();
    rowMeta.addValueMeta( new ValueMetaString( "pentahoField1" ) );
    rowMeta.addValueMeta( new ValueMetaString( "pentahoField2" ) );
    rowMeta.addValueMeta( new ValueMetaNumber( "pentahoNumber3" ) );
    rowMeta.addValueMeta( new ValueMetaBigNumber( "pentahoBigNumber4" ) );
    rowMeta.addValueMeta( new ValueMetaInternetAddress( "pentahoInet5" ) );
    rowMeta.addValueMeta( new ValueMetaBoolean( "pentahoBoolean6" ) );
    rowMeta.addValueMeta( new ValueMetaInteger( "pentahoInt7" ) );
    rowMeta.addValueMeta( new ValueMetaDate( "pentahoDate8" ) );
    rowMeta.addValueMeta( new ValueMetaTimestamp( "pentahoTimestamp9" ) );
    rowMeta.addValueMeta( new ValueMetaBinary( "pentahoBinary10" ) );

    rowData = new Object[][]
      { { "Row1Field1", "Row1Field2", 3.1, new BigDecimal( 4.1, MathContext.DECIMAL64 ),
        InetAddress.getByName( "1.2.3.4" ), true, 1L, new Date( 789 ), new Timestamp( 789 ), "foobar".getBytes() },

        { "Row2Field1", "Row2Field2", -3.2, new BigDecimal( -4.2, MathContext.DECIMAL64 ),
          InetAddress.getByName( "2.3.4.5" ), false, -2L, new Date( 789 ), new Timestamp( 789 ), "Donald Duck".getBytes() } };
  }

  @Test
  public void testOrcFileWriteAndRead() throws Exception {
    testRecordWriter();
    testRecordReader();
  }

  /**
   * Write two rows to orc file
   *
   * @throws Exception
   */

  private void testRecordWriter() throws Exception {
    IPentahoOutputFormat.IPentahoRecordWriter orcRecordWriter = orcOutputFormat.createRecordWriter();
    Assert.assertNotNull( orcRecordWriter, "orcRecordWriter should NOT be null!" );
    Assert.assertTrue( orcRecordWriter instanceof PentahoOrcRecordWriter,
      "orcRecordWriter should be instance of PentahoOrcRecordWriter" );

    orcRecordWriter.write( new RowMetaAndData( rowMeta, rowData[ 0 ] ) );
    orcRecordWriter.write( new RowMetaAndData( rowMeta, rowData[ 1 ] ) );
    orcRecordWriter.close();
  }

  /**
   * Read the rows back from Orc file
   *
   * @throws Exception
   */
  private void testRecordReader() throws Exception {

    PluginRegistry.addPluginType( ValueMetaPluginType.getInstance() );
    PluginRegistry.init( true );

    PentahoOrcInputFormat pentahoOrcInputFormat = new PentahoOrcInputFormat();
    pentahoOrcInputFormat.setSchema( schemaDescription );
    pentahoOrcInputFormat.setInputFile( filePath );
    IPentahoInputFormat.IPentahoRecordReader pentahoRecordReader = pentahoOrcInputFormat.createRecordReader( null );
    final AtomicInteger rowNumber = new AtomicInteger();
    for ( RowMetaAndData row : pentahoRecordReader ) {
      final AtomicInteger fieldNumber = new AtomicInteger();
      schemaDescription.forEach( field -> testValue( field, row, rowNumber, fieldNumber ) );
      rowNumber.incrementAndGet();
    }
  }

  private void testValue( SchemaDescription.Field field, RowMetaAndData row, AtomicInteger rowNumber, AtomicInteger fieldNumber ) {
    int fldNum = fieldNumber.getAndIncrement();
    int rowNum = rowNumber.get();
    if ( rowData[ rowNum ][ fldNum ] instanceof BigDecimal ) {
      assert ( ( (BigDecimal) rowData[ rowNum ][ fldNum ] ).compareTo(
        (BigDecimal) row.getData()[ fldNum ] ) == 0 );
    } else if ( rowData[ rowNum ][ fldNum ] instanceof byte[] ) {
      assertEquals( new String( (byte[]) rowData[ rowNum ][ fldNum ] ),
        new String( (byte[]) row.getData()[ fldNum ] ) );
    } else {
      assertEquals( rowData[ rowNum ][ fldNum ], row.getData()[ fldNum ] );
    }
  }
}
