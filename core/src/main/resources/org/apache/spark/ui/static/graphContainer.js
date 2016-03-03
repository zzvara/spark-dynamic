Spark.Visualizer.Model.Graph = (function() {

  // Containing the data for the graph.

  var
      // Each executor is a node of the graph.
      // The format of an executor is a map: {"exec_id" : exec_id, "host" : host, "name" : host + "_" + exec_id, "host_id" : host_id, "currentTasks" : [list of task_id -s and task_type -s pairs]}
      executors = [],
      // The links between executors. The source and target are the corresponding indeces in the executor array above.
      // The format of a link is map: {"source" : source, "target" : target, "linkThickness" : bytes}
      links = [],
      // taskToExecIndexMap: a map, task_id is the key and the corresponding index of the executor array is the value.
      taskToExecIndexMap = {},
      // execToExecIndexMap : a map, host + "_" + exec_id is the key and the corresponding index of the executor array is the value.
      execToExecIndexMap = {},

      execIndex = 0,

      // finding executor described by exec_id and host in executors array; returns boolean
      execAlready = function(exec_id, host) {
        var result = false;
        for (var j = 0; j < executors.length; ++j) {
          if (executors[j].exec_id == exec_id && executors[j].host == host) {
            result = true;
            break;
          }
        }
        return result;
      },

      bytesList = [],

      currentHostId = 0,

      hostToHostIdMap = {},

      runningTaskCounter = 0,

      completedTaskCounter = 0,

      getHostId = function(host) {
        var result;
        if (host in hostToHostIdMap) {
          result = hostToHostIdMap[host];
        } else {
          hostToHostIdMap[host] = currentHostId;
          result = currentHostId;
          currentHostId++;
        }
        return result;
      },

      // adding new executors or add new task to an already existing executor
      addLaunchEvent = function(taskLaunchEvent) {

        stageData = Spark.Visualizer.Model.needStageData(taskLaunchEvent);

        if (!(taskAlreadyLaunched(taskLaunchEvent.taskId))) {
          runningTaskCounter++;
        }

        taskLaunchEvent.creationSite = stageData.name.split(" at ")[1] || "unknown";
        taskLaunchEvent.operator = stageData.name.split(" at ")[0] || "unknown";
        taskLaunchEvent.shuffleId = stageData.shuffleId;
        taskLaunchEvent.rddInfo = stageData.rddInfo;

        //creationSiteInfo = creationSite.split(" at ")[1] || "";
        //taskName = creationSite.split(" at ")[0] || "";

        if (execAlready(taskLaunchEvent.executorId, taskLaunchEvent.host)) {
          // add new task to an already existing executor, maintaining taskToExecIndexMap with the new task
          executors[execToExecIndexMap[taskLaunchEvent.host + "_" + taskLaunchEvent.executorId]].currentTasks.push({"task_id" : taskLaunchEvent.taskId, "task_type" : taskLaunchEvent.operator,
            "stage": taskLaunchEvent.stageId, "partition": taskLaunchEvent.partitionId, "name": taskLaunchEvent.operator, "creationSite": taskLaunchEvent.creationSite, "rddId": taskLaunchEvent.rddData[taskLaunchEvent.rddData.length - 1].id,
            "finished": false});
          taskToExecIndexMap[taskLaunchEvent.taskId] = execToExecIndexMap[taskLaunchEvent.host + "_" + taskLaunchEvent.executorId];

        } else {
          // add new executor; maintaining taskToExecIndexMap, execToExecIndexMap, execIndex with the new executor
          var node = {"exec_id" : taskLaunchEvent.executorId, "host" : taskLaunchEvent.host, "name" : taskLaunchEvent.host + "_" + taskLaunchEvent.executorId, "host_id": getHostId(taskLaunchEvent.host),
            "currentTasks" : [{"task_id" : taskLaunchEvent.taskId, "task_type" : taskLaunchEvent.operator, "stage": taskLaunchEvent.stageId, "partition": taskLaunchEvent.partitionId,
              "name": taskLaunchEvent.operator, "creationSite": taskLaunchEvent.creationSite, "rddId": taskLaunchEvent.rddData[taskLaunchEvent.rddData.length - 1].id, "finished": false}], "x" : 0, "y" : 0 };
          executors.push(node);
          addInvisibleLinks(node);
          taskToExecIndexMap[taskLaunchEvent.taskId] = execIndex;
          execToExecIndexMap[taskLaunchEvent.host + "_" + taskLaunchEvent.executorId] = execIndex;
          ++execIndex;
        }
      },

      addInvisibleLinks = function(node) {
        for (var j = 0; j < executors.length; ++j) {
          if (executors[j].host_id == node.host_id) {
            links.push({"source" : node, "target" : executors[j], "linkThickness" : 1, "visibility" : "invisible"});
          }
        }
      },

      // finding link in links array; returns index, if not found returns -1
      findLink = function(sourceIndex, targetIndex) {
        var result = -1;
        for (var k = 0; k < links.length; ++k) {
          if (links[k].sourceIndex == sourceIndex && links[k].targetIndex == targetIndex && links[k].visibility == "visible") {
            result = k;
            break;
          }
        }
        return result;
      },

      // Searching and deleting task from executor's currentTask list.
      deleteTask = function(task_id) {
        var executor = executors[taskToExecIndexMap[task_id]];
        for (var j = 0; j < executor.currentTasks.length; ++j) {
          if (executor.currentTasks[j].task_id == task_id) {
            delete executor.currentTasks.splice(j);
          }
        }
      },

      // Set task status to finished.
      setTaskToFinished = function(task_id) {
        var executor = executors[taskToExecIndexMap[task_id]];
        for (var j = 0; j < executor.currentTasks.length; ++j) {
          if (executor.currentTasks[j].task_id == task_id) {
            executor.currentTasks[j].finished = true;
          }
        }
      },

      // Deciding whether task has already been launched.
      taskAlreadyLaunched = function(task_id) {
        var result = false;
        for (var j = 0; j < executors.length; ++j) {
          for (var i = 0; i < executors[j].currentTasks.length; ++i) {
            if (executors[j].currentTasks[i].task_id == task_id) {
              result = true;
            }
          }
        }

        return result;
      },

      // Deciding whether task has already benn finished.
      taskAlreadyFinished = function(task_id) {
        var result = false;
        for (var j = 0; j < executors.length; ++j) {
          for (var i = 0; i < executors[j].currentTasks.length; ++i) {
            if (executors[j].currentTasks[i].task_id == task_id && executors[j].currentTasks[i].finished == true) {
              result = true;
            }
          }
        }

        return result;
      },

      // Iterating through block requests and adding new links or updating link thickness of the already existing edge.
      addFinishEvent = function(taskFinishEvent) {

        if (taskAlreadyLaunched(taskFinishEvent.taskId) && !(taskAlreadyFinished(taskFinishEvent.taskId))) {
          completedTaskCounter++;
          runningTaskCounter--;
        }

        setTaskToFinished(taskFinishEvent.taskId);

        if (taskFinishEvent.metrics == undefined || taskFinishEvent.metrics.shuffleReadMetrics == undefined) {
          return;
        }

        var remoteBlockFetchInfos = taskFinishEvent.metrics.shuffleReadMetrics.remoteBlockFetchInfos;

        //iterating through block requests
        for (var j = 0; j < remoteBlockFetchInfos.length; ++j) {

          if (remoteBlockFetchInfos[j].host != "" && remoteBlockFetchInfos[j].executorId != "") {
            // determining source and target of link with the help of host, executor_id of the current block request and task_id
            var source = execToExecIndexMap[remoteBlockFetchInfos[j].host + "_" + remoteBlockFetchInfos[j].executorId];
            var target = taskToExecIndexMap[taskFinishEvent.taskId];
            // current block request's bytes into local variable
            var bytes = remoteBlockFetchInfos[j].bytes;

            // checking if current block request requires a new link or just adding bytes to an already existing link
            var linkFound = findLink(source, target);

            if (linkFound == -1) {
              // adding new link
              links.push({"source" : source, "target" : target, "linkThickness" : bytes,
                "visibility" : "visible", "name" : executors[source].name + "-" + executors[target].name,
                "sourceIndex" : source, "targetIndex" : target});
              bytesList.push(bytes);
            } else {
              // adding bytes to an existing link
              links[linkFound].linkThickness += bytes;
            }
          }
        }
      };

  return {

    getNnodes: function() {
      //ns.hull.data().length
      return Object.keys(hostToHostIdMap).length;
    },

    getNexecutors: function() {
      return executors.length;
    },

    getNrunningTasks: function() {
      return runningTaskCounter;
    },

    getNCompletedTasks: function() {
      return completedTaskCounter;
    },

    getExecutors: function() {
      return executors;
    },

    getLinks: function() {
      return links;
    },

    getBytesList: function() {
      return bytesList;
    },

    addLaunchEvent: addLaunchEvent,
    addFinishEvent: addFinishEvent,

  }
}());