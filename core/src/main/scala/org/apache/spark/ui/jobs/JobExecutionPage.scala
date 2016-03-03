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

package org.apache.spark.ui.jobs

import scala.xml.Node

import javax.servlet.http.HttpServletRequest

import org.apache.spark.ui.{ UIUtils, WebUIPage}

private[ui] class JobExecutionPage(parent: JobsTab) extends WebUIPage("execution") {
  def render(request: HttpServletRequest): Seq[Node] = {
    val listener = parent.jobProgresslistener

    // Do I really need jobProgressListener?
    listener.synchronized {
      val parameterId = request.getParameter("id")
      require(parameterId != null && parameterId.nonEmpty, "Missing id parameter")

      val jobId = parameterId.toInt

      val content =
        <nav class="Main" id="spark-exec-viz" data-app-name={parent.appId}>
          <button>
            Change view
          </button>
        </nav> ++
          <div class="Wrapper Runtime">
            <div id="Runtime">
              <div class="Current Time">
                <span class="Title">Elapsed time</span>
                <span class="Value">0 sec</span>
              </div>
              <ul class="Counters">
                <li class="Nodes">
                  <span>0</span>
                  nodes
                </li>
                <li class="Executors">
                  <span>0</span>
                  executors
                </li>
                <li class="Running Tasks">
                  <span>0</span>
                  running tasks
                </li>
                <li class="Completed Tasks">
                  <span>0</span>
                  completed tasks
                </li>
                <li class="Failed Tasks">
                  <span>0</span>
                  failed tasks
                </li>
              </ul>
            </div>
          </div> ++
          <div class="Wrapper Settings">
            <div id="Settings">
              <ul>
                <li>
                  <button class="Edge Scale">
                    <span>show network traffic in</span>
                    kilobytes
                  </button>
                </li>
                <li>
                  <button class="Global Scale In">
                    zoom in
                  </button>
                </li>
                <li>
                  <button class="Global Scale Out">
                    zoom out
                  </button>
                </li>
                <li>
                  <button class="Stop Runner">
                    stop
                  </button>
                </li>
                <li>
                  <button class="Edges All"
                          data-next-text="show all edges">
                    show only remote edges
                  </button>
                </li>
                <li>
                  <button class="Auto"
                          data-next-text="manual mode">
                    auto mode
                  </button>
                </li>
              </ul>
            </div>
          </div> ++
          <div class="Application">
            <ul class="Attributes">
              <li>
                <span>
                  cluster
                </span>
                <span>
                  random-cluster-name
                </span>
              </li>
              <li>
                <span>
                  application
                </span>
                <span>
                  random-application-name
                </span>
              </li>
              <li>
                <span>
                  deploy-mode
                </span>
                <span>
                  yarn-cluster
                </span>
              </li>
              <li>
                <span>
                  user
                </span>
                <span>
                  random-user-name
                </span>
              </li>
              <li class="Border">
                <span>
                  attribute
                </span>
                <span>
                  value
                </span>
              </li>
              <li>
                <span>
                  attribute
                </span>
                <span>
                  value
                </span>
              </li>
              <li>
                <span>
                  attribute
                </span>
                <span>
                  value
                </span>
              </li>
            </ul>
          </div> ++
          <div id="Dropzone">
            drop files here
          </div> ++
          <div id="Prototypes">
            <li class="Task Graph Element Prototype">
              <div>
                <div class="Header">
                  <span class="Stage">
                    <span>stage</span>
                    <span>
                      1
                    </span>
                  </span>
                  <span class="Partition">
                    <span>part</span>
                    <span>
                      6
                    </span>
                  </span>
                  <span class="Name">
                    map
                  </span>
                  <span class="Runtime">
                    23m
                  </span>
                  <span class="Creation Site">
                    at SparkPageRank.scala:61
                  </span>
                  -->
                </div>
                <div class="Body">
                  <ul>
                    <li>
                      <span class="Attribute">
                        Task ID
                      </span>
                      <span class="Task ID">
                        0
                      </span>
                    </li>
                    <li>
                      <span class="Attribute">
                        RDD ID
                      </span>
                      <span class="RDD ID">
                        5
                      </span>
                    </li>
                    <li>
                      <span class="Attribute">
                        RDD class
                      </span>
                      <span class="RDD Class">
                        org.apache.spark.rdd.MapPartitionsRDD
                      </span>
                    </li>
                  </ul>
                </div>
              </div>
            </li>
            <li class="Task Table Element Prototype">
              <div>
                <div class="Header">
                  <span class="Stage">
                    <span>stage</span>
                    <span>1</span>
                  </span>
                  <span class="Partition">
                    <span>part</span>
                    <span>6</span>
                  </span>
                  <span class="Name">
                    map
                  </span>
                  <span class="Runtime">
                    23m
                  </span>
                  <span class="Creation Site">
                    at SparkPageRank.scala:61
                  </span>
                </div>
                <div class="Body">
                  <ul>
                    <li class="Prototype Pipelined Inner RDD">
                      <span class="Name">
                        RDDInfo.scope.name
                      </span>
                      <span class="Connector">
                      </span>
                    </li>
                  </ul>
                  <div class="Histogram">
                  </div>
                </div>
              </div>
            </li>
            <div class="Bar Prototype">
              <span class="Key">
              </span>
              <span class="Value">
              </span>
            </div>
          </div>

      UIUtils.emptySparkPage(
        s"Execution graphs for Job $jobId",
        content,
        UIUtils.execVizHeaderNodes)
    }
  }
}