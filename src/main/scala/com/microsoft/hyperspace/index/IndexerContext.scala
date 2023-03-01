/*
 * Copyright (2021) The Hyperspace Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.microsoft.hyperspace.index

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

/**
 * Helper object for indexing operations for an index.
 */
trait IndexerContext {

  /**
   * Returns a Spark session.
   */
  def spark: SparkSession

  /**
   * Returns [[FileIdTracker]], which can be used to map files to numeric
   * identifiers which are unique in the scope of the index this context is
   * associated with.
   */
  def fileIdTracker: FileIdTracker

  /**
   * Returns the data path of the index this context is associated with.
   */
  def indexDataPath: Path

  /**
   * Returns index configuration.
   */
  def indexConfig: IndexConfigTrait = null
}
