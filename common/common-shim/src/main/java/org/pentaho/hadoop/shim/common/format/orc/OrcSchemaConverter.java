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
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.hadoop.shim.api.format.SchemaDescription;

/**
 * Converts a SchemaDescription to a Orc Structure
 * Created by tkafalas on 11/3/2017.
 */
public class OrcSchemaConverter {
  SchemaDescription schemaDescription;

  public TypeDescription buildTypeDescription( SchemaDescription schemaDescription ) {
    TypeDescription typeDescription = TypeDescription.createStruct();
    schemaDescription.forEach( f -> addStructField( typeDescription, f ) );
    return typeDescription;
  }

  private void addStructField( TypeDescription typeDescription, SchemaDescription.Field f ) {
    typeDescription.addField( f.formatFieldName, determineOrcType( f ) );
  }

  private TypeDescription determineOrcType( SchemaDescription.Field f ) {
    switch ( f.pentahoValueMetaType ) {
      case ValueMetaInterface.TYPE_NUMBER:
        return TypeDescription.createDouble();
      case ValueMetaInterface.TYPE_INET:
      case ValueMetaInterface.TYPE_STRING:
        return TypeDescription.createString();
      case ValueMetaInterface.TYPE_BOOLEAN:
        return TypeDescription.createBoolean();
      case ValueMetaInterface.TYPE_INTEGER:
        return TypeDescription.createLong();
      case ValueMetaInterface.TYPE_BIGNUMBER:
        return TypeDescription.createDecimal().withPrecision( 20 ).withScale( 10 );
      case ValueMetaInterface.TYPE_SERIALIZABLE:
        return TypeDescription.createBinary();
      case ValueMetaInterface.TYPE_BINARY:
        return TypeDescription.createBinary();
      case ValueMetaInterface.TYPE_DATE:
        return TypeDescription.createDate();
      case ValueMetaInterface.TYPE_TIMESTAMP:
        return TypeDescription.createTimestamp();
      default:
        throw new RuntimeException( "Field: " + f.formatFieldName + "  Undefined type: " + f.pentahoValueMetaType );
    }
  }
}
