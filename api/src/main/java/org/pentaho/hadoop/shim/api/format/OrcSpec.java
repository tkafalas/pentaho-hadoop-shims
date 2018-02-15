package org.pentaho.hadoop.shim.api.format;

import java.util.Arrays;
import java.util.ArrayList;

public class OrcSpec {
  public enum DataType {
    BOOLEAN( 0, true, "LongColumnVector", null, true, "Boolean" ),
    TINYINT( 1, true, "LongColumnVector", null, true, "TinyInt" ),
    SMALLINT( 2, true, "LongColumnVector", null, true, "SmallInt" ),
    INTEGER( 3, true, "LongColumnVector", null, true, "Int" ),
    BIGINT( 4, true, "LongColumnVector", null, true, "BigInt" ),
    BINARY( 5, true, "BytesColumnVector", null, true, "Binary" ),
    FLOAT( 6, true, "DoubleColumnVector", null, true, "Float" ),
    DOUBLE( 7, true, "DoubleColumnVector", null, true, "Double" ),
    DECIMAL( 8, true, "DecimalColumnVector", null, true, "Decimal" ),
    STRING( 9, true, "BytesColumnVector", null, true, "String" ),
    CHAR( 10, true, "BytesColumnVector", null, true, "Char" ),
    VARCHAR( 11, true, "BytesColumnVector", null, true, "VarChar" ),
    TIMESTAMP( 12, true, "TimestampColumnVector", null, true, "Timestamp" ),
    DATE( 13, true, "LongColumnVector", null, true, "Date" ),
    STRUCT( 14, true, "StructColumnVector", null, false, "Struct" ),
    LIST( 15, true, "ListColumnVector", null, false, "List" ),
    MAP( 16, true, "MapColumnVector", null, false, "List" ),
    UNION( 17, true, "UnionColumnVector", null, false, "Union" );

    private final int id;
    private final boolean isPrimitive;
    private final String baseType;
    private final String logicalType;
    private final boolean displayable;
    private final String name;
    private static final ArrayList<DataType> enumValues = new ArrayList<>();

    static {
      for ( DataType dataType : DataType.values() ) {
        enumValues.add( dataType.getId(), dataType );
      }
    }

    DataType( int id, boolean isPrimitiveType, String baseType, String logicalType, boolean displayable, String name ) {
      this.id = id;
      this.isPrimitive = isPrimitiveType;
      this.baseType = baseType;
      this.logicalType = logicalType;
      this.displayable = displayable;
      this.name = name;
    }

    public static DataType getDataType( int id ) {
      return enumValues.get( id );
    }

    public int getId() {
      return this.id;
    }

    public boolean isPrimitiveType() {
      return isPrimitive;
    }

    public boolean isComplexType() {
      return !isPrimitive && ( logicalType == null );
    }

    public boolean isLogicalType() {
      return logicalType != null;
    }

    public String getBaseType() {
      return baseType;
    }

    public String getLogicalType() {
      return logicalType;
    }

    public String getType() {
      return isLogicalType() ? logicalType : baseType;
    }

    public boolean isDisplayable() {
      return this.displayable;
    }

    public String getName() {
      return name;
    }

    public static String[] getDisplayableTypeNames() {
      return Arrays.stream( OrcSpec.DataType.values() )
        .filter( dataType -> dataType.isDisplayable() )
        .map( dataType -> dataType.getName() )
        .sorted()
        .toArray( String[]::new );
    }
  }
}
