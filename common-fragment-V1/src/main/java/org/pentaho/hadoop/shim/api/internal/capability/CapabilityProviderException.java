/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2021 by Hitachi Vantara : http://www.pentaho.com
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
package org.pentaho.hadoop.shim.api.internal.capability;

import org.pentaho.di.core.Const;

/**
 * Indicates a runtime error occurred while processing capabilities
 */
public class CapabilityProviderException extends RuntimeException {

  public CapabilityProviderException( String message, Exception ex ) {
    super( message, ex );
  }

  public CapabilityProviderException( Exception ex ) {
    super( ex );
  }

  /**
   * Constructs a new throwable with the specified detail message.
   *
   * @param message - the detail message. The detail message is saved for later retrieval by the getMessage() method.
   */
  public CapabilityProviderException( String message ) {
    super( message );
  }

  /**
   * get the messages back to it's origin cause.
   */
  @Override
  public String getMessage() {
    String retval = Const.CR;
    retval += super.getMessage() + Const.CR;

    Throwable cause = getCause();
    if ( cause != null ) {
      String message = cause.getMessage();
      if ( message != null ) {
        retval += message + Const.CR;
      } else {
        // Add with stack trace elements of cause...
        StackTraceElement[] ste = cause.getStackTrace();
        for ( int i = ste.length - 1; i >= 0; i-- ) {
          retval +=
            " at "
              + ste[ i ].getClassName() + "." + ste[ i ].getMethodName() + " (" + ste[ i ].getFileName() + ":"
              + ste[ i ].getLineNumber() + ")" + Const.CR;
        }
      }
    }

    return retval;
  }
}

