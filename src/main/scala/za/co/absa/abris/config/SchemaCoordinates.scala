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

import io.confluent.kafka.serializers.subject.{RecordNameStrategy, TopicNameStrategy, TopicRecordNameStrategy}
import org.apache.avro.Schema

trait SchemaCoordinates

class IdCoordinate(val schemaId: Int) extends SchemaCoordinates

class SubjectCoordinate(val subject: String, val version: Option[Int]) extends SchemaCoordinates

object SubjectCoordinate {
  private val TOPIC_NAME_STRATEGY = new TopicNameStrategy()
  private val RECORD_NAME_STRATEGY = new RecordNameStrategy()
  private val TOPIC_RECORD_NAME_STRATEGY = new TopicRecordNameStrategy()

  def fromTopicNameStrategy(
    topicName: String,
    version: Option[Int],
    isKey: Boolean = false
  ): SubjectCoordinate = {
    val dummySchema = Schema.createRecord("name", "", "namespace", false)
    new SubjectCoordinate(TOPIC_NAME_STRATEGY.subjectName(topicName, isKey, dummySchema), version)
  }

  def fromRecordNameStrategy(
    recordName: String,
    recordNamespace: String,
    version: Option[Int],
    isKey: Boolean = false
  ): SubjectCoordinate = {
    val dummySchema = Schema.createRecord(recordName, "", recordNamespace, false)
    new SubjectCoordinate(RECORD_NAME_STRATEGY.subjectName("", isKey, dummySchema), version)
  }

  def fromRecordNameStrategy(
    schema: Schema,
    isKey: Boolean
  ): SubjectCoordinate = {
    new SubjectCoordinate(RECORD_NAME_STRATEGY.subjectName("", isKey, schema), None)
  }

  def fromTopicRecordNameStrategy(
    topicName: String,
    recordName: String,
    recordNamespace: String,
    version: Option[Int],
    isKey: Boolean = false
  ): SubjectCoordinate = {
    val dummySchema = Schema.createRecord(recordName, "", recordNamespace, false)
    new SubjectCoordinate(TOPIC_RECORD_NAME_STRATEGY.subjectName(topicName, isKey, dummySchema), version)
  }

  def fromTopicRecordNameStrategy(
    topicName: String,
    schema: Schema,
    isKey: Boolean
  ): SubjectCoordinate = {
    new SubjectCoordinate(TOPIC_RECORD_NAME_STRATEGY.subjectName(topicName, isKey, schema), None)
  }
}
