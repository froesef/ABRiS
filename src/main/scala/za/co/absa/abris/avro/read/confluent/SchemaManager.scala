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

package za.co.absa.abris.avro.read.confluent

import java.security.InvalidParameterException

import io.confluent.kafka.schemaregistry.client.{SchemaMetadata, SchemaRegistryClient}
import org.apache.avro.Schema
import org.apache.spark.internal.Logging
import za.co.absa.abris.avro.parsing.utils.AvroSchemaUtils

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class SchemaManager(schemaRegistryClient: SchemaRegistryClient) extends Logging {

  def getSchemaById(schemaId: Int): Schema = schemaRegistryClient.getById(schemaId)

  /**
   * @param version - Some(versionNumber) or None for latest version
   */
  def getSchemaBySubjectAndVersion(subject: String, version: Option[Int]): Schema = {
    val metadata = getSchemaMetadataBySubjectAndVersion(subject, version)

    AvroSchemaUtils.parse(metadata.getSchema)
  }

  /**
   * @param version - Some(versionNumber) or None for latest version
   */
  def getSchemaMetadataBySubjectAndVersion(subject: String, version: Option[Int]): SchemaMetadata =
    version
      .map(schemaRegistryClient.getSchemaMetadata(subject, _))
      .getOrElse(schemaRegistryClient.getLatestSchemaMetadata(subject))

  def register(subject: String, schemaString: String): Int = register(subject, AvroSchemaUtils.parse(schemaString))

  /**
   * Register a new schema for a subject if the schema is compatible with the latest available version.
   *
   * @return registered schema id
   */
  def register(subject: String, schema: Schema): Int = {
    if (!exists(subject) || isCompatible(schema, subject)) {
      logInfo(s"AvroSchemaUtils.registerIfCompatibleSchema: Registering schema for subject: $subject")
      schemaRegistryClient.register(subject, schema)
    } else {
      throw new InvalidParameterException(s"Schema could not be registered for subject '${subject}'. " +
        "Make sure that the Schema Registry is available, the parameters are correct and the schemas are compatible")
    }
  }

  /**
   * Checks if a given schema exists in Schema Registry.
   */
  def exists(subject: String): Boolean = {
    Try(schemaRegistryClient.getLatestSchemaMetadata(subject)) match {
      case Success(_) => true
      case Failure(e) if e.getMessage.contains("Subject not found") || e.getMessage.contains("No schema registered") =>
        logInfo(s"Subject not registered: '$subject'")
        false
      case Failure(e) =>
        logError(s"Problems found while retrieving metadata for subject '$subject'", e)
        false
    }
  }

  /**
   * Checks if a new schema is compatible with the latest schema registered for a given subject.
   */
  private def isCompatible(newSchema: Schema, subject: String): Boolean = {
    schemaRegistryClient.testCompatibility(subject, newSchema)
  }

  def getAllSchemasWithMetadata(subject: String): List[SchemaMetadata] = {
    val versions = schemaRegistryClient.getAllVersions(subject).asScala.toList
    versions.map(schemaRegistryClient.getSchemaMetadata(subject, _))
  }

  def findEquivalentSchema(schema: Schema, subject: String): Option[Int] = {
    val maybeIdenticalSchemaMetadata =
      getAllSchemasWithMetadata(subject)
        .find{
          sm => AvroSchemaUtils.parse(sm.getSchema).equals(schema)
        }

    maybeIdenticalSchemaMetadata.map(_.getId)
  }

  def getIfExistsOrElseRegisterSchema(schema: Schema, subject: String): Int = {
    val maybeSchemaId = findEquivalentSchema(schema, subject)
    maybeSchemaId.getOrElse(register(subject, schema))
  }
}

/**
  * This object provides methods to integrate with remote schemas through Schema Registry.
  *
  * This can be considered an "enriched" facade to the Schema Registry client.
  *
  * This is NOT THREAD SAFE, which means that multiple threads operating on this object
  * (e.g. calling 'configureSchemaRegistry' with different parameters) would operated
  * on the same Schema Registry client, thus, leading to inconsistent behavior.
  */
object SchemaManager extends Logging {

  val PARAM_SCHEMA_REGISTRY_TOPIC = "schema.registry.topic"
  val PARAM_SCHEMA_REGISTRY_URL = "schema.registry.url"
  val PARAM_VALUE_SCHEMA_ID = "value.schema.id"
  val PARAM_VALUE_SCHEMA_VERSION = "value.schema.version"
  val PARAM_KEY_SCHEMA_ID = "key.schema.id"
  val PARAM_KEY_SCHEMA_VERSION = "key.schema.version"
  val PARAM_SCHEMA_ID_LATEST_NAME = "latest"

  val PARAM_KEY_SCHEMA_NAMING_STRATEGY = "key.schema.naming.strategy"
  val PARAM_VALUE_SCHEMA_NAMING_STRATEGY = "value.schema.naming.strategy"

  val PARAM_KEY_SCHEMA_NAME_FOR_RECORD_STRATEGY = "key.schema.name"
  val PARAM_KEY_SCHEMA_NAMESPACE_FOR_RECORD_STRATEGY = "key.schema.namespace"

  val PARAM_VALUE_SCHEMA_NAME_FOR_RECORD_STRATEGY = "value.schema.name"
  val PARAM_VALUE_SCHEMA_NAMESPACE_FOR_RECORD_STRATEGY = "value.schema.namespace"

  object SchemaStorageNamingStrategies extends Enumeration {
    val TOPIC_NAME = "topic.name"
    val RECORD_NAME = "record.name"
    val TOPIC_RECORD_NAME = "topic.record.name"
  }
}

class SchemaManagerException(msg: String, throwable: Throwable) extends RuntimeException(msg, throwable) {
  def this(msg: String) = this(msg, null)
}
