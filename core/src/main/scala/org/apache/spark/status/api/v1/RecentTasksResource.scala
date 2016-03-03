/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.status.api.v1

import javax.ws.rs.{PathParam, GET, Produces}
import javax.ws.rs.core.MediaType

import org.apache.spark.ui.SparkUI
import org.apache.spark.ui.jobs.UIData.TaskUIData

@Produces(Array(MediaType.APPLICATION_JSON))
private[v1] class RecentTasksResource(ui: SparkUI) {

  @GET
  def recentTaskStatuses(@PathParam("since") since: Long): Iterable[TaskData] = {
    RecentTasksResource.recentlyUpdatedTaskData(ui, since).map {
      AllStagesResource.convertTaskData
    }
  }
}

private[v1] object RecentTasksResource {
  /**
    * In any stage, search for tasks, which has been launched or finished since the given time.
    */
  def recentlyUpdatedTaskData(ui: SparkUI, since: Long) : Iterable[TaskUIData] = {
    val listener = ui.jobProgressListener
    listener.stageIdToData.values.flatMap {
      _.taskData.values.filter {
        taskData =>
          taskData.taskInfo.finishTime > since || taskData.taskInfo.launchTime > since
      }
    }
  }
}