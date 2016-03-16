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

Array.prototype.distinct = function() {
    return this.reduce(function(p, c) {
        if (p.indexOf(c) < 0) p.push(c);
        return p;
    }, []);
};

var Spark = { };

Spark.Configuration = {
    api_version: "1"
};

Spark.Utils = {
    parseDate: function(date) {
        return Date.parse(date.slice(0, -4));
    },

    getParameterByName: function(name) {
        name = name.replace(/[\[]/, "\\[").replace(/[\]]/, "\\]");
        var
            regex = new RegExp("[\\?&]" + name + "=([^&#]*)"),
            results = regex.exec(location.search);
        return results === null ? "" : decodeURIComponent(results[1].replace(/\+/g, " "));
    },

    cloneNode: function(selector) {
        return d3.select(selector).node().cloneNode(true);
    },

    convertHex: function(hex, opacity) {
        hex = hex.replace('#','');
        r = parseInt(hex.substring(0,2), 16);
        g = parseInt(hex.substring(2,4), 16);
        b = parseInt(hex.substring(4,6), 16);
        result = 'rgba('+r+','+g+','+b+','+opacity/100+')';
        return result;
    },

    /**
     * @todo Use Date object instead if can.
     * @param miliSec
     * @returns {string}
     */
    timestampToHumanReadable: function(miliSec) {
        result = "";
        denominators = [86400000, 3600000, 60000, 1000];
        quotients = [0, 0, 0, 0];
        timeNames = [" day", " hour", " min", " sec"];
        for (var j = 0; j < denominators.length; ++j) {
            quotients[j] = Math.floor(miliSec / denominators[j]);
            miliSec = miliSec % denominators[j];
        }
        for(var j = denominators.length - 1; j > -1; --j) {
            if (quotients[j] > 0) {
                var item = quotients[j] + timeNames[j];
                result = item.concat(", ", result);
            }
        }
        if(result.length > 2) {
            result = result.slice(0, result.length - 2);
        }
        return result;
    },

    getElapsedTime: function(beforeTime, afterTime) {
        beforeTime = Spark.Utils.parseDate(beforeTime);
        if(typeof afterTime !== 'string') {
            afterTime = Date.now();
        } else {
            afterTime = Spark.Utils.parseDate(afterTime);
        }
        return afterTime - beforeTime;
    },

    elapsedTimeToHumanReadable: function(elapsed) {
        if(elapsed < 0) elapsed = 0;
        names = ["s", "m", "h", "d"];
        scale = [0, 60, 60, 24];
        d = elapsed;
        i = 0;
        while(Math.floor(d / scale[i + 1]) > 0) {
            d = d / scale[i + 1];
            i++;
        }
        return Math.floor(d) + names[i];
    },

    getElapsedTimeHumanReadable: function(beforeTime, afterTime) {
        difference = Math.floor((Spark.Utils.getElapsedTime(beforeTime, afterTime)) / 1000);
        return Spark.Utils.elapsedTimeToHumanReadable(difference);
    },

    /**
     * @todo Document!
     * @param obj
     * @returns {*}
     */
    clone: function(obj) {
        if (null == obj || "object" != typeof obj) return obj;
        var copy = obj.constructor();
        for (var attr in obj) {
            if (obj.hasOwnProperty(attr)) copy[attr] = obj[attr];
        }
        return copy;
    }
};

Spark.Visualizer = {};

Spark.Visualizer.Execution = {
    GRAPH_VIEW: "GRAPH_VIEW",
    TABLE_VIEW: "TABLE_VIEW"
};

Spark.Visualizer.Execution.View = {
    refreshStats: function(stats) {
        d3.select(".Counters .Nodes SPAN")
            .text(stats.nNodes);
        d3.select(".Counters .Executors SPAN")
            .text(stats.nExecutors);
        d3.select(".Counters .Running.Tasks SPAN")
            .text(stats.nRunningTasks);
        d3.select(".Counters .Completed.Tasks SPAN")
            .text(stats.nCompletedTasks);
    },
    colors : [ "#0f5959", "#340F59", "#0F1859", "#590F3D", "#3D590F", "#0F5950" ]
};


Spark.Visualizer.Execution.View.Colorize = (function() {
    var
        groups = {},

        init = function() {

        };

    init();

    return {
        getColorForGroup: function(entity, group) {
            var index = 0;

            if(group in groups) {
                if($.inArray(entity, groups[group]) !== -1) {
                    index = groups[group].indexOf(entity);
                } else {
                    groups[group].push(entity);
                    index = groups[group].length - 1;
                }
            } else {
                groups[group] = [ entity ];
                index = 0;
            }

            return Spark.Visualizer.Execution.View.colors[index];
        },
        getGroups: function() {
            return groups;
        }
    }
}());

Spark.Visualizer.Execution.Control = {};

Spark.Visualizer.Model = {
    stageData: [],
    taskData: [],

    needStageData: function(taskLaunchEvent) {
        if (!(taskLaunchEvent.stageId in Spark.Visualizer.Model.stageData)) {
            throw new Error("Stage " + taskLaunchEvent.stageId + " data not found for task " +
                taskLaunchEvent.taskId + "!");
        }
        return Spark.Visualizer.Model.stageData[taskLaunchEvent.stageId];
    }
};

Spark.Visualizer.Execution.Reader = function(eventHandler) {
    var
        handler = eventHandler,
        feeder,

        applicationId,
        jobId = Spark.Utils.getParameterByName("id"),
        driverHostName = Spark.Utils.getParameterByName("driverHostName"),

        init = function() {
            if (jobId != "") {
                console.info("Detected job ID is: " + jobId + ".");
            } else {
                console.warn("Job ID is not detected. Going to use default: 0.");
                jobId = "0";
            }

            if (driverHostName != "") {
                console.info("Detected driver host name is: " + driverHostName + ".");
            }

            var url = driverHostName + "/api/v1/applications";

            $.ajax({
                dataType: "json",
                url: url,
                beforeSend: function(jqXHR, settings) {
                    console.debug("Fetching application ID from Spark REST API [" + url + "]");
                },
                success: function(data) {
                    applicationId = data[0].id;
                    console.debug("Application name fetched successfully: " + applicationId + ".");
                    /**
                     * @todo Job ID should be fetched dynamically.
                     */
                    initializeFeeders();
                },
                error: function() {
                    console.error("Error while fetching application ID from Spark [" + url + "]!");
                }
            });

        },

        initializeFeeders = function() {
            console.debug("Initializing data feeders.");
            taskDataFeeder = new Spark.Visualizer.Execution.Feeder({
                dataReader: taskDataReader,
                requestInterval: 3000,
                resourceParameters: {
                    appId: applicationId,
                    jobId: jobId,
                    driverHostName: driverHostName
                },
                destination:
                    "{driverHostName}/api/v1/applications/{appId}/{jobId}/tasks/{timestamp}"
            }).start();

            stageDataFeeder = new Spark.Visualizer.Execution.Feeder({
                dataReader: stageDataReader,
                resourceParameters: {
                    appId: applicationId,
                    jobId: jobId,
                    driverHostName: driverHostName
                },
                destination:
                    "{driverHostName}/api/v1/applications/{appId}/stages"
            });
        },

        taskDataReader = function(taskArray) {

            taskArray.sort(function(taskA, taskB) {
                taskIdDiff = taskA.taskId - taskB.taskId;
                return (taskIdDiff == 0) ?
                    (Spark.Utils.parseDate(taskA.finishTime) -
                    Spark.Utils.parseDate(taskB.finishTime)) : taskIdDiff
            });

            var taskArrayHandleCallback = function() {
                taskData = Spark.Visualizer.Model.taskData;
                $.each(taskArray, function(index, task) {
                    if(task.taskId in taskData) {
                        if(task.status == "RUNNING") {
                            console.warn("Duplicate record for task with ID " + task.taskId + "!")
                        } else {
                            handler(createFinishedData(task));
                        }
                    } else if(task.status != "RUNNING") {
                        handler(createLaunchedData(task));
                        handler(createFinishedData(task));
                    } else {
                        handler(createLaunchedData(task));
                    }
                    Spark.Visualizer.Model.taskData[task.taskId] = task;
                });
            };

            if(taskArray.length == 0) {
                console.log("No need to handle task data, it is empty.");
                return;
            } else {
                console.log(taskArray);
                console.log("Reading and handling task data (size of " + taskArray.length + ").");
            }
            stageData = Spark.Visualizer.Model.stageData;

            var stageDataMissing = false;
            for(var i = 0; i < taskArray.length; i++) {
                if(!(taskArray[i].stageId in stageData)) {
                    console.log("Stage data for stage " + taskArray[i].stageId + " is not present. " +
                        "Tryling to load it.");
                    stageDataFeeder.run(taskArrayHandleCallback);
                    stageDataMissing = true;
                    break;
                }
            }
            if(!stageDataMissing) {
                taskArrayHandleCallback();
            }
        },

        createFinishedData = function(task) {
            console.debug("Creating TaskFinished event.");

            stageData = Spark.Visualizer.Model.stageData;

            return {
                event: "TaskFinished",
                taskId: task.taskId,
                partitionId: task.index,
                stageId: task.stageId,
                attempt: task.attempt,
                speculative: task.speculative,
                locality: task.taskLocality,
                status: task.status,
                finishTime: task.finishTime,
                launchTime: task.launchTime,
                metrics: task.taskMetrics,
                stageData: usefulStageData(stageData[task.stageId])
            }
        },

        createLaunchedData = function(task) {
            console.debug("Creating TaskLaunched event.");

            stageData = Spark.Visualizer.Model.stageData;

            return {
                event: "TaskLaunched",
                taskId: task.taskId,
                partitionId: task.index,
                stageId: task.stageId,
                attempt: task.attempt,
                speculative: task.speculative,
                locality: task.taskLocality,
                status: task.status,
                launchTime: task.launchTime,
                stageData: usefulStageData(stageData[task.stageId]),
                executorId: task.executorId,
                host: task.host
            }
        },

        stageDataReader = function(stageArray) {
            console.log("Reading and handling stage data.");

            $.each(stageArray, function(index, stage) {
                stage.rddData.sort(function(x, y) { return x.id - y.id });

                Spark.Visualizer.Model.stageData[stage.stageId] = stage;
            });
        },

        usefulStageData = function(stageData) {
            return stageData;
        };

    init();

    return {
        taskDataReader : taskDataReader
    }
};

/**
 * Generic feeder that can fetch independently or periodically resources
 * from Spark's REST API to a data reader.
 * @param configuration Configuration object.
 * @returns {{start: Function, stop: Function, reset: Function, run: Function}}
 * @constructor
 */
Spark.Visualizer.Execution.Feeder = function(configuration) {
    var

        /**
         * Function that handles fetched resource from Spark API.
         * @param data Supplied with configuration.
         */
        dataReader = function(data){},
        /**
         * Interval in which requests are created.
         * @type {number} Supplied with configuration.
         */
        requestInterval = 1000,
        /**
         * The CSS selector to the DOM node, where the resource parameters
         * should be fetched from.
         * There should be a {{data-app-name}} and a {{data-job-id}} set as attribute.
         * @type {string} Supplied with configuration.
         */
        resourceParametersFrom,
        /**
         * Maximum attempts to fetch resource before stopping.
         * @type {number} Supplied with configuration.
         */
        maxAttempts = 5,
        /**
         * Destination URL to fetch the resource from.
         * The following patterns will be replaced:
         *  - '{appName}' Application name.
         *  - '{jobId}' Job ID.
         *  - '{timestamp}' This will be replaced with the time the last request occured.
         * @type {string} Supplied with configuration.
         */
        destination,

        /**
         * Request parameters read from {{resourceParametersFrom}}.
         * @type {object}
         */
        requestParameters = configuration.resourceParameters,
        /**
         * The time of the last request, to be able to retrieve updates.
         * @type {number}
         */
        lastRequestedTimestamp = 0,
        /**
         * AJAX request handler.
         * @type {object}
         */
        $requestHandler,
        /**
         * If an ongoing request is pending.
         * @type {boolean}
         */
        haveActiveRequest = false,
        /**
         * Counter for request attempts.
         * @type {number}
         */
        currentAttempt = 1,
        /**
         * If the feeder is running in interval mode.
         * @type {boolean}
         */
        isRunning = false,

        init = function(configuration) {
            readConfiguration(configuration)
        },

        run = function(onSuccess) {
            requestData(onSuccess);
        },

        start = function() {
            if(isRunning) {
                console.warn("Feeder is already running!");
                return;
            }
            isRunning = true;
            readHandler = window.setInterval(requestData, requestInterval);
            return this;
        },

        reset = function() {
            stop();
            start();
        },

        stop = function() {
            if(!isRunning) {
                console.warn("Feeder is not running!");
                return;
            }
            isRunning = false;
            window.clearInterval(readHandler);
        },

        requestData = function(onSuccess) {
            onSuccess = typeof onSuccess === 'function' ? onSuccess : function(){};

            if(!haveActiveRequest) {
                var url = buildDestination();
                $requestHandler = $.ajax({
                    dataType: "json",
                    url: url,
                    beforeSend: function(jqXHR, settings) {
                        console.debug("Requesting data from Spark REST API [" + url + "]");
                        haveActiveRequest = true;
                    },
                    success: function(data) {
                        console.debug("Request success.");
                        currentAttempt = 1;
                        inactivateRequest();
                        dataReader(prepareData(data));
                        onSuccess();
                    },
                    error: function() {
                        console.error("Error while requesting data from Spark [" + url + "]" +
                            "(attempt " + currentAttempt + ")!");
                        inactivateRequest();
                        if(currentAttempt < maxAttempts) {
                            currentAttempt++;
                        } else {
                            console.warn("Stopping Feeder!");
                            stop();
                        }
                    },
                    complete: function() {
                        if(isRunning &&
                            (new Date()).getTime() > lastRequestedTimestamp + requestInterval) {
                            console.log("Resetting Feeder timer.");
                            reset();
                        }
                    }
                });
            }
        },

        buildDestination = function() {
            path = destination;
            path = path.replace("{appId}", requestParameters.appId);
            path = path.replace("{jobId}", requestParameters.jobId);
            path = path.replace("{driverHostName}", requestParameters.driverHostName);
            path = path.replace("{timestamp}", getAndUpdateRequestTimestamp());
            return path;
        },

        inactivateRequest = function() {
            haveActiveRequest = false;
        },

        getAndUpdateRequestTimestamp = function() {
            savedLastTimestamp = lastRequestedTimestamp;
            lastRequestedTimestamp = (new Date()).getTime();
            return savedLastTimestamp;
        },

        prepareData = function(data) {
            return data;
        },

        readConfiguration = function(configuration) {
            if(typeof(configuration) === 'object') {
                if(typeof(configuration.dataReader) === 'function') {
                    dataReader = configuration.dataReader;
                }

                if(typeof configuration.requestInterval === 'number') {
                    requestInterval = configuration.requestInterval;
                }

                if(typeof configuration.maxAttempts === 'number') {
                    maxAttempts = configuration.maxAttempts;
                }

                if(typeof configuration.destination === 'string') {
                    destination = configuration.destination;
                    var dummyAnchor = document.createElement('a');
                    dummyAnchor.href = window.location.href;
                    var splitPath = dummyAnchor.pathname.split("/");
                    if(splitPath[1] == "proxy") {
                        console.debug("YARN detected.");
                        destination = "/" + splitPath[1] + "/" + splitPath[2] + destination;
                        console.debug(destination);
                    } else if(splitPath[1] == "history") {
                        console.debug("History server detected.");
                        requestParameters.appName = splitPath[2];
                    }
                } else {
                    console.error("Destination is invalid!");
                }
            }
        };

    init(configuration);

    return {
        start: start,
        stop: stop,
        reset: reset,
        run: run,
        isRunning: function() {
            return isRunning;
        },
        haveActiveRequest: function() {
            return haveActiveRequest;
        },
        currentAttempt: function() {
            return currentAttempt;
        }
    }
};
