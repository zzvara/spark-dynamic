Spark.Visualizer.Model.Table = (function () {
    var
    // the holder of tables (one table with its tasks in an array tables.tasks.) 
        tables = [],
        tableIndex = 0,

        tableIndexToRddIdMap = {},

        tableLevelX = 0,
        tableLevelY = 0,

        tableIndexToStagePartMap = {},

    // the holder of links (one link with the source and target task id)
        links = [],
        bytesList = [],
        shufflePartToTaskIdMap = {},
        taskIdToTableIndexMap = {},
        rddIdPartToTaskIdMap = {},
        copyCount = 0,

        tableStageToTasks = {},

        findLink = function (sourceTaskId, targetTaskId) {
            var result = -1;
            for (var i = 0; i < links.length; ++i) {
                if (links[i].sourceTaskId == sourceTaskId && links[i].targetTaskId == targetTaskId) {
                    result = i;
                    break;
                }
            }
            return result;
        },

    // Sometimes we copy a column from an already existing table, and add a new table with this column.
        copyColumn = function (tableKey, rddId) {
            ++copyCount;
            var table = tables[tableKey];
            var copiedTaskList = [];
            var copiedTask;

            tableIndexToStagePartMap[tableIndex] = {};
            for (var k = 0; k < table.tasks.length; ++k) {
                if (table.tasks[k].rddData[table.tasks[k].rddData.length - 1].id == rddId) {
                    copiedTask = Spark.Utils.clone(table.tasks[k]);
                    for (var l = 0; l < copyCount; ++l) {
                        copiedTask.ID += "_c";
                    }

                    copiedTaskList.push(copiedTask);
                    copiedTask.tableIndex = tableIndex;

                    addToTableIndex(tableIndex, copiedTask);

                    if (tableIndexToStagePartMap[tableIndex][copiedTask.stageId] == undefined) {
                        tableIndexToStagePartMap[tableIndex][copiedTask.stageId] = [];
                    }
                    tableIndexToStagePartMap[tableIndex][copiedTask.stageId].push(copiedTask.partitionId);

                    shufflePartToTaskIdMap[copiedTask.shuffleId + "_" + copiedTask.partitionId].unshift(copiedTask.ID);
                    rddIdPartToTaskIdMap[copiedTask.rddData[copiedTask.rddData.length - 1].id + "_" + copiedTask.partitionId].unshift(copiedTask.ID);
                    taskIdToTableIndexMap[copiedTask.ID] = tableIndex;
                }
            }
            tables.push({
                tasks: copiedTaskList,
                tableLevelX: tableLevelX,
                tableLevelY: tableLevelY,
                tableIndex: tableIndex
            });
            ++tableIndex;
            ++tableLevelY;
        },

    // Set task status to finished.
        setTaskToFinished = function (taskFinishedEvent) {
            var tableIndex = taskIdToTableIndexMap[taskFinishedEvent.taskId];
            var tasks = tables[tableIndex].tasks;
            for (var i = 0; i < tasks.length; ++i) {
                if (tasks[i].ID == taskFinishedEvent.taskId) {
                    tasks[i].finished = true;
                    tasks[i].finishTime = taskFinishedEvent.finishTime;
                    tasks[i].histogram =
                        (typeof taskFinishedEvent.metrics != "undefined" &&
                        typeof taskFinishedEvent.metrics.shuffleReadMetrics != "undefined")
                            ? taskFinishedEvent.metrics.shuffleReadMetrics.dataCharacteristics
                            : [];

                    break;
                }
            }
        },

        addToTableIndex = function (tableIndex, task) {
            var key = tableIndex + "_" + task.stageId;
            var tasks = tableStageToTasks[key];

            if (tasks == undefined) {
                tableStageToTasks[key] = [];
                tasks = tableStageToTasks[key];
            }

            var executor = (task.host + "-" + task.executorId);

            var isPlaced = false;
            for (var i = 0; i < tasks.length; ++i) {
                if (executor.localeCompare(tasks[i]["executor"]) < 0) {
                    tableStageToTasks[key].splice(i, 0, {
                        "executor": executor,
                        "ID": task.ID
                    });
                    isPlaced = true;
                    break;
                }
            }
            if (!isPlaced) {
                tableStageToTasks[key].push({
                    "executor": executor,
                    "ID": task.ID
                });
            }
        },

        getInnerLinkSources = function (ID) {
            var result = [];
            for (var i = 0; i < links.length; ++i) {
                if (links[i].targetTaskId == ID) {
                    result.push(links[i].sourceTaskId);
                }
            }
            return result;
        },

        fillMaxStagePart = function () {

            var maxStage = [];
            var maxPart = [];

            for (var i = 0; i < tables.length; ++i) {

                var stagesNum = Object.keys(tableIndexToStagePartMap[i]).length;
                var partNum = 0;
                for (var stage in tableIndexToStagePartMap[i]) {
                    if (partNum < tableIndexToStagePartMap[i][stage].length) {
                        partNum =  tableIndexToStagePartMap[i][stage].length;
                    }
                }

                var levelX = tables[i].tableLevelX;
                var levelY = tables[i].tableLevelY;

                if ((maxStage[levelX] == undefined) || (stagesNum > maxStage[levelX])) {
                    maxStage[levelX] = stagesNum;
                }
                if ((maxPart[levelY] == undefined) || (partNum > maxPart[levelY])) {
                    maxPart[levelY] = partNum;
                }

            }

            return [maxStage, maxPart];

        },

        fillTablePos = function () {

            var stagePartMatrix = [];

            /**
             * @todo Maybe no need for distinct?
             */
            for (var i = 0; i < tables.length; ++i) {

                var stagesNum = Object.keys(tableIndexToStagePartMap[i]).length;
                var partNum = 0;
                for (var stage in tableIndexToStagePartMap[i]) {
                    if (partNum < tableIndexToStagePartMap[i][stage].length) {
                        partNum =  tableIndexToStagePartMap[i][stage].length;
                    }
                }

                var levelX = tables[i].tableLevelX;
                var levelY = tables[i].tableLevelY;

                if (stagePartMatrix.length < levelX + 1) {
                    stagePartMatrix.push([]);
                }

                stagePartMatrix[levelX][levelY] = {"stagesNum": stagesNum, "partNum": partNum};

                tables[i].posXstages = [];
                for (var k = 0; k < levelX; ++k) {
                    tables[i].posXstages.push(stagePartMatrix[k][levelY].stagesNum);
                }
                tables[i].posYparts = [];
                for (var l = 0; l < levelY; ++l) {
                    tables[i].posYparts.push(stagePartMatrix[levelX][l].partNum);
                }
            }
        },

        addLaunchEvent = function (taskLaunchEvent) {
            stageData = Spark.Visualizer.Model.needStageData(taskLaunchEvent);

            taskLaunchEvent.creationSite = stageData.name.split(" at ")[1] || "unknown";
            taskLaunchEvent.operator = stageData.name.split(" at ")[0] || "unknown";
            taskLaunchEvent.shuffleId = stageData.shuffleId;
            taskLaunchEvent.rddData = stageData.rddData;

            /**
             * @todo Maybe generate unique ID.
             */
            taskLaunchEvent.ID = taskLaunchEvent.taskId;

            /**
             * @todo array of array instead of double key maps.
             */
            shufflePartToTaskIdMap[taskLaunchEvent.shuffleId + "_" + taskLaunchEvent.partitionId] = [taskLaunchEvent.taskId];
            rddIdPartToTaskIdMap[taskLaunchEvent.rddData[taskLaunchEvent.rddData.length - 1].id + "_" + taskLaunchEvent.partitionId] = [taskLaunchEvent.taskId];

            var taskPlaced = false;

            for (var tableKey in tableIndexToRddIdMap) {
                if (taskLaunchEvent.rddData[taskLaunchEvent.rddData.length - 1].id == tableIndexToRddIdMap[tableKey][0]) {
                    taskLaunchEvent.tableIndex = tableKey;
                    addToTableIndex(tableKey, taskLaunchEvent);
                    tables[tableKey].tasks.push(taskLaunchEvent);

                    if (tableIndexToStagePartMap[tableKey][taskLaunchEvent.stageId] == undefined) {
                        tableIndexToStagePartMap[tableKey][taskLaunchEvent.stageId] = [];
                    }
                    tableIndexToStagePartMap[tableKey][taskLaunchEvent.stageId].push(taskLaunchEvent.partitionId);

                    taskIdToTableIndexMap[taskLaunchEvent.ID] = tableKey;
                    taskPlaced = true;
                    break;
                }
            }

            if (!taskPlaced) {

                nestedLoop:
                    for (var i = 0; i < taskLaunchEvent.rddData.length; ++i) {
                        for (var tableKey in tableIndexToRddIdMap) {
                            if (taskLaunchEvent.rddData[i].id - 1 == tableIndexToRddIdMap[tableKey][0]) {

                                taskLaunchEvent.tableIndex = tableKey;
                                addToTableIndex(tableKey, taskLaunchEvent);
                                tables[tableKey].tasks.push(taskLaunchEvent);
                                tableIndexToRddIdMap[tableKey].unshift(taskLaunchEvent.rddData[taskLaunchEvent.rddData.length - 1].id);

                                if (tableIndexToStagePartMap[tableKey][taskLaunchEvent.stageId] == undefined) {
                                    tableIndexToStagePartMap[tableKey][taskLaunchEvent.stageId] = [];
                                }
                                tableIndexToStagePartMap[tableKey][taskLaunchEvent.stageId].push(taskLaunchEvent.partitionId);

                                taskIdToTableIndexMap[taskLaunchEvent.ID] = tableKey;
                                taskPlaced = true;
                                break nestedLoop;
                            } else {
                                for (var j = 1; j < tableIndexToRddIdMap[tableKey].length; ++j) {
                                    if (taskLaunchEvent.rddData[i].id - 1 == tableIndexToRddIdMap[tableKey][j]) {
                                        copyColumn(tableKey, tableIndexToRddIdMap[tableKey][j]);

                                        ++tableLevelX;
                                        tableLevelY = 0;

                                        taskLaunchEvent.tableIndex = tableIndex;
                                        addToTableIndex(tableIndex, taskLaunchEvent);
                                        tables.push({
                                            tasks: [taskLaunchEvent],
                                            tableLevelX: tableLevelX,
                                            tableLevelY: tableLevelY,
                                            tableIndex: tableIndex
                                        });
                                        tableIndexToRddIdMap[tableIndex] = [taskLaunchEvent.rddData[taskLaunchEvent.rddData.length - 1].id];
                                        tableIndexToStagePartMap[tableIndex] = {};
                                        if (tableIndexToStagePartMap[tableIndex][taskLaunchEvent.stageId] == undefined) {
                                            tableIndexToStagePartMap[tableIndex][taskLaunchEvent.stageId] = [];
                                        }
                                        tableIndexToStagePartMap[tableIndex][taskLaunchEvent.stageId].push(taskLaunchEvent.partitionId);

                                        taskIdToTableIndexMap[taskLaunchEvent.ID] = tableIndex;
                                        ++tableIndex;
                                        ++tableLevelY;

                                        taskPlaced = true;
                                        break nestedLoop;
                                    }
                                }
                            }
                        }
                    }
            }

            if (!taskPlaced) {
                taskLaunchEvent.tableIndex = tableIndex;
                addToTableIndex(tableIndex, taskLaunchEvent);
                tables.push({
                    tasks: [taskLaunchEvent],
                    tableLevelX: tableLevelX,
                    tableLevelY: tableLevelY,
                    tableIndex: tableIndex
                });
                tableIndexToRddIdMap[tableIndex] = [taskLaunchEvent.rddData[taskLaunchEvent.rddData.length - 1].id];
                tableIndexToStagePartMap[tableIndex] = {};
                if (tableIndexToStagePartMap[tableIndex][taskLaunchEvent.stageId] == undefined) {
                    tableIndexToStagePartMap[tableIndex][taskLaunchEvent.stageId] = [];
                }
                tableIndexToStagePartMap[tableIndex][taskLaunchEvent.stageId].push(taskLaunchEvent.partitionId);

                taskIdToTableIndexMap[taskLaunchEvent.ID] = tableIndex;
                ++tableIndex;
                ++tableLevelY;
            }

            fillTablePos();
        },

        addFinishEvent = function (taskFinishEvent) {

            setTaskToFinished(taskFinishEvent);

            if (taskFinishEvent.metrics == undefined || taskFinishEvent.metrics.shuffleReadMetrics == undefined) {
                return;
            }

            var remoteBlockFetchInfos = taskFinishEvent.metrics.shuffleReadMetrics.remoteBlockFetchInfos;
            var localBlockFetchInfos = taskFinishEvent.metrics.shuffleReadMetrics.localBlockFetchInfos;
            var blockFetchInfos = taskFinishEvent.metrics.blockFetchInfos;

            var processBlockFetchInfos = function (blockFetchInfos, linkType) {
                if (typeof blockFetchInfos == "undefined") {
                    return;
                }
                for (var j = 0; j < blockFetchInfos.length; ++j) {
                    var executor = blockFetchInfos[j].executorId;
                    var host = blockFetchInfos[j].host;
                    var targetTableIndex = taskIdToTableIndexMap[taskFinishEvent.taskId];
                    var bytes = blockFetchInfos[j].bytes;
                    var blockId = blockFetchInfos[j].blockId;
                    var sourceTaskId;
                    var sourceTableIndex;

                    if (!(blockId.rddId == undefined)) {
                        //var rddId = blockId.rddId;
                        var rddId = blockId.rddId - 1;
                        var rddPartitionId = blockId.splitIndex;

                        sourceTaskId = rddIdPartToTaskIdMap[rddId + "_" + rddPartitionId][0];
                        sourceTableIndex = taskIdToTableIndexMap[sourceTaskId];
                    } else {
                        var shuffleId = blockId.shuffleId;
                        var partitionId = blockId.mapId;

                        sourceTaskId = shufflePartToTaskIdMap[shuffleId + "_" + partitionId][0];
                        sourceTableIndex = taskIdToTableIndexMap[sourceTaskId];
                    }

                    var linkFound = findLink(sourceTaskId, taskFinishEvent.taskId);
                    if (linkFound == -1) {
                        links.push({
                            sourceTaskId: sourceTaskId, targetTaskId: taskFinishEvent.taskId,
                            sourceTableIndex: sourceTableIndex, targetTableIndex: targetTableIndex,
                            linkThickness: bytes, linkType: linkType
                        });
                        bytesList.push(bytes);
                    } else {
                        links[linkFound].linkThickness += bytes;
                        bytesList[linkFound] += bytes;
                    }
                }
            };

            processBlockFetchInfos(remoteBlockFetchInfos, "remote");
            processBlockFetchInfos(localBlockFetchInfos, "local");
            processBlockFetchInfos(blockFetchInfos, "local");

        };

    return {
        addLaunchEvent: addLaunchEvent,
        addFinishEvent: addFinishEvent,
        getTables: function () {
            return tables;
        },
        getTableIndexToStagePartMap: function () {
            return tableIndexToStagePartMap;
        },
        getTasksByTableStage: function (tableID, stageID) {
            return tableStageToTasks[tableID + "_" + stageID];
        },
        getBytesList: function () {
            return bytesList;
        },
        getTaskIdToTableIndexMap: function () {
            return taskIdToTableIndexMap;
        },
        findTableLink: findLink,
        getTableLinks: function () {
            return links;
        },
        getInnerLinkSources: getInnerLinkSources,
        fillMaxStagePart: fillMaxStagePart
    }
}());