
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.bwsw.tstreamstransactionserver.options.loader

import java.io.FileInputStream
import java.util.Properties

import com.bwsw.tstreamstransactionserver.options.CommonOptions

object PropertyFileLoader {
  private val fileName = CommonOptions.PROPERTY_FILE_NAME

  def apply(): PropertyFileLoader = {
    val propertyFileOpt =
      Option(System.getProperty(fileName))

    propertyFileOpt match {
      case None =>
        throw new IllegalArgumentException(
          s"There is no file with properties. " +
            s"You should define a path to a property file through " +
            s"'-D$fileName=<path_to_file>'(e.g. 'java -D$fileName=/home/user/config.properties')."
        )
      case Some(file) =>
        val properties = new Properties()
        properties
          .load(new FileInputStream(file))
        new PropertyFileLoader(properties)
    }
  }
}


class PropertyFileLoader(properties: Properties) {
  def castCheck[T](property: String,
                   constructor: String => T
                  )(implicit classType: Class[_]): T = {
    val value = checkPropertyExists(property)(classType)
    try {
      constructor(value)
    } catch {
      case _: IllegalArgumentException =>
        throw new IllegalArgumentException(
          s"Property '$property' has got an invalid format, but expected another type."
        )
    }
  }

  def checkPropertyExists(property: String)(classType: Class[_]): String = {
    Option(properties.getProperty(property))
      .getOrElse(throw new NoSuchElementException(
        s"No property by key: '$property' has been found for '${classType.getSimpleName}'." +
          s"You should define it and restart the program.")
      )
  }
}
