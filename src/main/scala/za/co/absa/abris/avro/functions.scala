/*
 * Copyright 2019 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.abris.avro

import org.apache.spark.sql.Column
import za.co.absa.abris.avro.sql.{AvroDataToCatalyst, CatalystDataToAvro}
import za.co.absa.abris.config.{FromAvroConfig, ToAvroConfig}


// scalastyle:off: object.name
object functions {
// scalastyle:on: object.name
// scalastyle:off: method.name

  def to_avro(data: Column, config: ToAvroConfig): Column = {
    new Column(CatalystDataToAvro(
      data.expr,
      config.schemaString,
      config.schemaId
    ))
  }

  def from_avro(data: Column, config: FromAvroConfig): Column = {
    new Column(AvroDataToCatalyst(
      data.expr,
      config.schemaString,
      config.schemaRegistryConf,
      config.schemaRegistryConf.isDefined
    ))
  }

  // scalastyle:on: method.name
}
