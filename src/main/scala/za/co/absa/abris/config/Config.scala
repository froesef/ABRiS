/*
 * Copyright 2018 ABSA Group Limited
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

package za.co.absa.abris.config

import za.co.absa.abris.avro.parsing.utils.AvroSchemaUtils
import za.co.absa.abris.avro.read.confluent.SchemaManagerFactory

object AbrisConfig {
  def toSimpleAvro: ToSimpleAvroConfigFragment = new ToSimpleAvroConfigFragment()
  def toConfluentAvro: ToConfluentAvroConfigFragment = new ToConfluentAvroConfigFragment()
  def fromSimpleAvro: FromSimpleAvroConfigFragment = new FromSimpleAvroConfigFragment()
  def fromConfluentAvro: FromConfluentAvroConfigFragment = new FromConfluentAvroConfigFragment()

  val SCHEMA_REGISTRY_URL = "schema.registry.url"
}

/*
 * ================================================  To Avro ================================================
 */

class ToSimpleAvroConfigFragment() {
  def downloadSchemaById(schemaId: Int): ToSchemaDownloadingConfigFragment =
    new ToSchemaDownloadingConfigFragment(new IdCoordinate(schemaId), false)

  def downloadSchemaByLatestVersion: ToStrategyConfigFragment =
    new ToStrategyConfigFragment(None, false)

  def downloadSchemaByVersion(schemaVersion: Int): ToStrategyConfigFragment =
    new ToStrategyConfigFragment(Some(schemaVersion), false)

  def provideSchema(schema: String): ToAvroConfig =
    new ToAvroConfig(schema, None)

  def provideAndRegisterSchema(schema: String): ToConfluentAvroRegistrationStrategyConfigFragment =
    new ToConfluentAvroRegistrationStrategyConfigFragment(schema, false)
}

class ToStrategyConfigFragment(version: Option[Int], confluent: Boolean) {
  def andTopicNameStrategy(
    topicName: String,
    isKey: Boolean = false
  ): ToSchemaDownloadingConfigFragment =
    new ToSchemaDownloadingConfigFragment(
      SubjectCoordinate.fromTopicNameStrategy(topicName, version, isKey), confluent)

  def andRecordNameStrategy(
    recordName: String,
    recordNamespace: String,
    isKey: Boolean = false
  ): ToSchemaDownloadingConfigFragment =
    new ToSchemaDownloadingConfigFragment(
      SubjectCoordinate.fromRecordNameStrategy(recordName, recordNamespace, version, isKey), confluent)


  def andTopicRecordNameStrategy(
    topicName: String,
    recordName: String,
    recordNamespace: String,
    isKey: Boolean = false
  ): ToSchemaDownloadingConfigFragment =
    new ToSchemaDownloadingConfigFragment(
      SubjectCoordinate.fromTopicRecordNameStrategy(topicName, recordName, recordNamespace, version, isKey), confluent)
}

class ToSchemaDownloadingConfigFragment(schemaCoordinates: SchemaCoordinates, confluent: Boolean) {
  def usingSchemaRegistry(url: String): ToAvroConfig = usingSchemaRegistry(Map(AbrisConfig.SCHEMA_REGISTRY_URL -> url))
  def usingSchemaRegistry(config: Map[String, String]): ToAvroConfig = {
    val schemaManager = SchemaManagerFactory.create(config)
    val (schemaId, schemaString) = schemaCoordinates match {
      case ic: IdCoordinate => (ic.schemaId, schemaManager.getSchemaById(ic.schemaId).toString)
      case sc: SubjectCoordinate => {
        val metadata = schemaManager.getSchemaMetadataBySubjectAndVersion(sc.subject, sc.version)
        (metadata.getId, metadata.getSchema)
      }
    }
    new ToAvroConfig(schemaString, if (confluent) Some(schemaId) else None)
  }
}

object Foo {
  AbrisConfig
    .toSimpleAvro
    .downloadSchemaByVersion(4)
    .andTopicNameStrategy("fooTopic")
    .usingSchemaRegistry("http://registry")

  AbrisConfig
    .toConfluentAvro
    .provideAndRegisterSchema("schema foo")
    .usingTopicNameStrategy("topic1")
    .usingSchemaRegistry("url")

  AbrisConfig
    .toConfluentAvro
    .provideAndRegisterSchema("schema")
    .usingTopicNameStrategy("fooTopic")
    .usingSchemaRegistry("registryUrl")

}

class ToConfluentAvroConfigFragment() {
  def downloadSchemaById(schemaId: Int): ToSchemaDownloadingConfigFragment =
    new ToSchemaDownloadingConfigFragment(new IdCoordinate(schemaId), true)

  def downloadSchemaByLatestVersion: ToStrategyConfigFragment =
    new ToStrategyConfigFragment(None, true)

  def downloadSchemaByVersion(schemaVersion: Int): ToStrategyConfigFragment =
    new ToStrategyConfigFragment(Some(schemaVersion), true)

  def provideAndRegisterSchema(schema: String): ToConfluentAvroRegistrationStrategyConfigFragment =
    new ToConfluentAvroRegistrationStrategyConfigFragment(schema, true)
}

class ToConfluentAvroRegistrationStrategyConfigFragment(schema: String, confluent: Boolean) {
  def usingTopicNameStrategy(
    topicName: String,
    isKey: Boolean = false
  ): ToSchemaRegisteringConfigFragment =
    new ToSchemaRegisteringConfigFragment(
      SubjectCoordinate.fromTopicNameStrategy(topicName, None, isKey), schema, true)

  def usingRecordNameStrategy(
    isKey: Boolean = false
  ): ToSchemaRegisteringConfigFragment =
    new ToSchemaRegisteringConfigFragment(
      SubjectCoordinate.fromRecordNameStrategy(AvroSchemaUtils.parse(schema), isKey), schema, true)

  def usingTopicRecordNameStrategy(
    topicName: String,
    isKey: Boolean = false
  ): ToSchemaRegisteringConfigFragment =
    new ToSchemaRegisteringConfigFragment(
      SubjectCoordinate.fromTopicRecordNameStrategy(topicName, AvroSchemaUtils.parse(schema), isKey), schema, true)
}

class ToSchemaRegisteringConfigFragment(
  subjectCoordinate: SubjectCoordinate,
  schemaString: String,
  confluent: Boolean
) {
  def usingSchemaRegistry(url: String): ToAvroConfig = usingSchemaRegistry(Map(AbrisConfig.SCHEMA_REGISTRY_URL -> url))
  def usingSchemaRegistry(config: Map[String, String]): ToAvroConfig = {
    val schemaManager = SchemaManagerFactory.create(config)
    val schema = AvroSchemaUtils.parse(schemaString)
    val schemaId = schemaManager.getIfExistsOrElseRegisterSchema(schema, subjectCoordinate.subject)
    new ToAvroConfig(schemaString, if (confluent) Some(schemaId) else None)
  }
}

class ToAvroConfig(val schemaString: String, val schemaId: Option[Int])

/*
 * ================================================  From Avro ================================================
 */

class FromSimpleAvroConfigFragment{
  def downloadSchemaById(schemaId: Int): FromSchemaDownloadingConfigFragment =
    new FromSchemaDownloadingConfigFragment(Left(new IdCoordinate(schemaId)), false)

  def downloadSchemaByLatestVersion: FromStrategyConfigFragment =
    new FromStrategyConfigFragment(None, false)

  def downloadSchemaByVersion(schemaVersion: Int): FromStrategyConfigFragment =
    new FromStrategyConfigFragment(Some(schemaVersion), false)

  def provideSchema(schema: String): FromAvroConfig =
    new FromAvroConfig(schema, None)
}

class FromStrategyConfigFragment(version: Option[Int], confluent: Boolean) {
  def andTopicNameStrategy(
    topicName: String,
    isKey: Boolean = false
  ): FromSchemaDownloadingConfigFragment = {
    val coordinate = SubjectCoordinate.fromTopicNameStrategy(topicName, version, isKey)
    new FromSchemaDownloadingConfigFragment(Left(coordinate), confluent)
  }

  def andRecordNameStrategy(
    recordName: String,
    recordNamespace: String,
    isKey: Boolean = false
  ): FromSchemaDownloadingConfigFragment = {
    val coordinate = SubjectCoordinate.fromRecordNameStrategy(recordName, recordNamespace, version, isKey)
    new FromSchemaDownloadingConfigFragment(Left(coordinate), confluent)
  }

  def andTopicRecordNameStrategy(
    topicName: String,
    recordName: String,
    recordNamespace: String,
    isKey: Boolean = false
  ): FromSchemaDownloadingConfigFragment = {
    val coordinate = SubjectCoordinate.fromTopicRecordNameStrategy(
      topicName, recordName, recordNamespace, version, isKey)

    new FromSchemaDownloadingConfigFragment(Left(coordinate), confluent)
  }
}

class FromSchemaDownloadingConfigFragment(
  schemaCoordinatesOrSchemaString: Either[SchemaCoordinates, String],
  confluent: Boolean
) {
  def usingSchemaRegistry(url: String): FromAvroConfig =
    usingSchemaRegistry(Map(AbrisConfig.SCHEMA_REGISTRY_URL -> url))

  def usingSchemaRegistry(config: Map[String, String]): FromAvroConfig = schemaCoordinatesOrSchemaString match {
    case Left(coordinate) => {
      val schemaManager = SchemaManagerFactory.create(config)
      val schema = coordinate match {
        case ic: IdCoordinate => schemaManager.getSchemaById(ic.schemaId)
        case sc: SubjectCoordinate => schemaManager.getSchemaBySubjectAndVersion(sc.subject, sc.version)
      }
      new FromAvroConfig(schema.toString, if (confluent) Some(config) else None)
    }
    case Right(schemaString) =>
      if (confluent) {
        new FromAvroConfig(schemaString, Some(config))
      } else {
        throw new UnsupportedOperationException("Unsupported config permutation")
      }
  }
}

class FromConfluentAvroConfigFragment {
  def downloadReaderSchemaById(schemaId: Int): FromSchemaDownloadingConfigFragment =
    new FromSchemaDownloadingConfigFragment(Left(new IdCoordinate(schemaId)), true)

  def downloadReaderSchemaByLatestVersion: FromStrategyConfigFragment =
    new FromStrategyConfigFragment(None, true)

  def downloadReaderSchemaByVersion(schemaVersion: Int): FromStrategyConfigFragment =
    new FromStrategyConfigFragment(Some(schemaVersion), true)

  def provideSchema(schema: String): FromSchemaDownloadingConfigFragment =
    new FromSchemaDownloadingConfigFragment(Right(schema), true)
}


class FromAvroConfig(val schemaString: String, val schemaRegistryConf: Option[Map[String,String]])
