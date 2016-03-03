Spark.Visualizer.Execution.View.Table = (function () {
    // Global parameters of the visualization.
    // svgContainer and svgTable are the main div and svg tag of the html. All other tags will be attached to them.
    // attributes and params are imported from separate classes below. Params are constant numbers and strings controlling the visualization.
    // Attributes are various attibutes of the html tags of the visualization.
    // scaleLevels holds some ratio of the possible size scales of the visualization.
    var
        svgContainer,
        svgTable,
        tables,
        attributes = Spark.Visualizer.Execution.View.Attributes,
        params = Spark.Visualizer.Execution.View.Params,
        scaleLevels = [1, 0.9, 0.8, 0.7, 0.6, 0.5, 0.4],
        currentScale = 0,

        // Adding the main div and svg tag of the html.
        init = function () {
            // Here we add the main div html element for the table.
            svgContainer = d3.select("body").append("div")
                .attr(attributes.tableAttr.svgContainer);

            // Here we add the main svg html element for the table.
            svgTable = svgContainer.append("svg")
                .attr(attributes.tableAttr.svgTable)
                .attr("transform", "translate(40,20)");

            // Adding Arrow marker of the links.
            svgTable.append("defs").append("marker")
                .attr(attributes.tableAttr.marker)
                .append("path")
                .attr(attributes.tableAttr.markerPath);

            redraw();
        },

        // Counting the width of different tables according to the number of stages inside them.
        setTableRectWidth = function (d, i) {
            var result = 0;
            var numOfStages = Object.keys(Spark.Visualizer.Model.Table.getTableIndexToStagePartMap()[i]).length;
            var offset = params.tableParams.taskOffSetX;
            var taskWidth = params.tableParams.taskRectWidth;
            result = numOfStages * (2 * offset + taskWidth);
            return result;
        },

        // Counting the height of different tables according to the maximal partition inside them.
        setTableRectHeight = function (d, i) {
            var result = 0;

            var tableIndexToStagePartMap = Spark.Visualizer.Model.Table.getTableIndexToStagePartMap();
            var numOfParts = 0;
            for (var stage in tableIndexToStagePartMap[i]) {
                if (numOfParts < tableIndexToStagePartMap[i][stage].length) {
                    numOfParts =  tableIndexToStagePartMap[i][stage].length;
                }
            }

            var offset = params.tableParams.taskOffSetY;
            var taskHeight = params.tableParams.taskRectHeight;
            result = numOfParts * (2 * offset + taskHeight);

            return result;
        },

        // Transform size of the bytes to human readable form.
        getReadableSize = function (sizeInBytes) {
            var prefixNames = [' B', ' kB', ' MB', ' GB', ' TB'];
            var denominators = [1, 1024, Math.pow(1024, 2), Math.pow(1024, 3), Math.pow(1024, 4)];
            sizeInBytes = sizeInBytes / denominators[params.graphParams.bytePrefixId];
            return sizeInBytes.toFixed(1) + prefixNames[params.graphParams.bytePrefixId];
        },

        // Parsing coordinates from transform attribute string.
        parseTransformAttr = function (str) {
            var strParsed = str.split("(")[1].split(",");
            var transX = strParsed[0];
            var transY = strParsed[1].split(")")[0];
            return [parseInt(transX), parseInt(transY)];
        },

        // Positioning tables according to max stage partition matrix heuristics.
        tableTranslate = function (d) {

            var transX = params.tableParams.initialTransX;
            var transY = params.tableParams.initialTransY;

            var maxStage = Spark.Visualizer.Model.Table.fillMaxStagePart()[0];
            var maxPart = Spark.Visualizer.Model.Table.fillMaxStagePart()[1];

            var oneStageWidth = (2 * params.tableParams.taskOffSetX + params.tableParams.taskRectWidth);
            var onePartHeight = (2 * params.tableParams.taskOffSetY + params.tableParams.taskRectHeight);

            for (var i = 0; i < d.tableLevelX; ++i) {
                transX += maxStage[i] * oneStageWidth + params.tableParams.tableOffSetX;
            }
            transX += (maxStage[d.tableLevelX] - Object.keys(Spark.Visualizer.Model.Table.getTableIndexToStagePartMap()[d.tableIndex]).length) * oneStageWidth;

            for (var i = 0; i < d.tableLevelY; ++i) {
                transY += maxPart[i] * onePartHeight + params.tableParams.tableOffSetY;
            }
            transY += (maxPart[0] * onePartHeight / 2) * d.tableLevelX;

            return "translate(" + transX + "," + transY + ")";
        },

        // Getting the task vertical position.
        getTaskYPos = function (tableIndex, stageId, ID) {
            var result = 0;
            var tasks = Spark.Visualizer.Model.Table.getTasksByTableStage(tableIndex, stageId);
            for (var i = 0; i < tasks.length; ++i) {
                if (ID == tasks[i]["ID"]) {
                    result = i;
                    break;
                }
            }
            return result;
        },

        // Positioning the tasks.
        taskTranslate = function (d) {
            var transX = 0;
            var transY = 0;
            var offsetX = params.tableParams.taskOffSetX;
            var offsetY = params.tableParams.taskOffSetY;
            var taskWidth = params.tableParams.taskRectWidth;
            var taskHeight = params.tableParams.taskRectHeight;

            var tableIndexToStagePartMap = Spark.Visualizer.Model.Table.getTableIndexToStagePartMap();
            var maxPart = 0;
            for (var stage in tableIndexToStagePartMap[d.tableIndex]) {
                if (maxPart < tableIndexToStagePartMap[d.tableIndex][stage].length) {
                    maxPart = tableIndexToStagePartMap[d.tableIndex][stage].length;
                }
            }

            var stages = Object.keys(tableIndexToStagePartMap[d.tableIndex]).map(function(stage) {
                return parseInt(stage);
            });

            var stagePos = stages.sort(function (x, y) {
                return x - y;
            }).indexOf(d.stageId);

            var partitions = tableIndexToStagePartMap[d.tableIndex][d.stageId];

            var maxPartLength = maxPart * (2 * offsetY + taskHeight);
            var currentPartitionsLength = partitions.length * (2 * offsetY + taskHeight);

            var initialOffSetY = (maxPartLength - currentPartitionsLength) / 2;

            transX = offsetX + (2 * offsetX + taskWidth) * stagePos;
            transY = offsetY + initialOffSetY + (2 * offsetY + taskHeight) * getTaskYPos(d.tableIndex, d.stageId, d.ID);

            return "translate(" + transX + "," + transY + ")";
        },

        // Getting the link start and end point coordinates according to their source and target tasks.
        getLinkX1 = function (d) {

            var sourceTaskRect = d3.select("#tableTask" + d.sourceTaskId);
            var sourceTable = d3.select("#tableGroup_" + d.sourceTableIndex);

            var taskTransX = parseTransformAttr(sourceTaskRect.attr("transform"))[0];
            var tableTransX = parseTransformAttr(sourceTable.attr("transform"))[0];

            return taskTransX + tableTransX + params.tableParams.taskRectWidth;
        },

        getLinkX2 = function (d) {
            var targetTaskRect = d3.select("#tableTask" + d.targetTaskId);
            var targetTable = d3.selectAll("#tableGroup_" + d.targetTableIndex);

            var taskTransX = parseTransformAttr(targetTaskRect.attr("transform"))[0];
            var tableTransX = parseTransformAttr(targetTable.attr("transform"))[0];

            return taskTransX + tableTransX;
        },

        getLinkY1 = function (d) {
            var sourceTaskRect = d3.select("#tableTask" + d.sourceTaskId);
            var sourceTable = d3.select("#tableGroup_" + d.sourceTableIndex);
            var taskTransY = parseTransformAttr(sourceTaskRect.attr("transform"))[1];
            var tableTransY = parseTransformAttr(sourceTable.attr("transform"))[1];

            return taskTransY + tableTransY + params.tableParams.taskRectHeight / 2;
        },

        getLinkY2 = function (d) {
            var targetTaskRect = d3.select("#tableTask" + d.targetTaskId);
            var targetTable = d3.select("#tableGroup_" + d.targetTableIndex);
            var taskTransY = parseTransformAttr(targetTaskRect.attr("transform"))[1];
            var tableTransY = parseTransformAttr(targetTable.attr("transform"))[1];

            return taskTransY + tableTransY + params.tableParams.taskRectHeight / 2;
        },

        // Positioning stage labels.
        getStageLabelX = function (d, i) {
            var result = 0;
            var offsetX = params.tableParams.taskOffSetX;
            var taskWidth = params.tableParams.taskRectWidth;
            result = offsetX + (taskWidth * 0.33) + (2 * offsetX + taskWidth) * i;
            return result;
        },

        // Positioning stage labels.
        getStageLabelY = function (d, i) {
            return (-1) * params.tableParams.taskOffSetY;
        },

        // Scaling the thickness of the links.
        thicknessScale = function (value) {
            var scale = d3.scale.linear()
                .domain([1, d3.max(Spark.Visualizer.Model.Table.getBytesList())])
                .range([params.tableParams.minThickness, params.tableParams.maxThickness]);
            return scale(value);
        },

        // Drawing the link info label during highlighting.
        drawEndPointLabel = function (sourceTaskId, targetTaskId) {
            var tableIndex = Spark.Visualizer.Model.Table.getTaskIdToTableIndexMap()[sourceTaskId];
            var table = d3.selectAll("#tableGroup_" + tableIndex);
            var tableX = parseTransformAttr(table.attr("transform"))[0];
            var tableY = parseTransformAttr(table.attr("transform"))[1];

            var sourceTask = d3.select("#tableTask" + sourceTaskId);
            var sourceX = parseTransformAttr(sourceTask.attr("transform"))[0];
            var sourceY = parseTransformAttr(sourceTask.attr("transform"))[1];
            var linkFound = Spark.Visualizer.Model.Table.findTableLink(sourceTaskId, targetTaskId);
            var linkLabelTextContent = getReadableSize(Spark.Visualizer.Model.Table.getTableLinks()[linkFound].linkThickness);

            var linkLabel = svgTable.append("g")
                .attr(attributes.tableAttr.linkLabelGroup);

            linkLabel.append("rect")
                .attr(attributes.tableAttr.linkLabelRect)
                .attr("id", "linkLabelRect" + sourceTaskId + "-" + targetTaskId);

            var linkLabelText = linkLabel.append("text")
                .attr(attributes.tableAttr.linkLabelText)
                .attr("x", sourceX + tableX + params.tableParams.taskRectWidth + params.tableParams.linkLabelOffsetX)
                .attr("y", sourceY + tableY + (params.tableParams.taskRectHeight / 2))
                .text(linkLabelTextContent);

            d3.selectAll("#linkLabelRect" + sourceTaskId + "-" + targetTaskId)
                .attr("width", linkLabelText[0][0].getBBox().width + 10)
                .attr("height", linkLabelText[0][0].getBBox().height + 10)
                .attr("x", linkLabelText[0][0].getBBox().x - 5)
                .attr("y", linkLabelText[0][0].getBBox().y - 5);

        },

        // Highlight inward links of the task, when hover.
        highlightLink = function (d) {
            d3.selectAll(".tableLink.line")
                .style("opacity", 0.2)
                .style("stroke", "grey");

            d3.selectAll(".tableLink.line[id$=\"-" + d.ID + "\"]")
                .style("opacity", 1)
                .style("stroke", "#0F5959");

            var innerLinkSources = Spark.Visualizer.Model.Table.getInnerLinkSources(d.ID);

            for (var i = 0; i < innerLinkSources.length; ++i) {
                drawEndPointLabel(innerLinkSources[i], d.ID);
            }
        },

        // Return to original state from highlighting.
        cancelHighlightLink = function (d) {
            d3.selectAll(".tableLink.line")
                .style("opacity", 1)
                .style("stroke", "#0F5959");

            svgTable.selectAll(".tableLinkLabel.text").remove();
            svgTable.selectAll(".tableLinkLabel.rect").remove();
            svgTable.selectAll(".tableLinkLabel.group").remove();

        },

        // Controlling the zoom.
        rescale = function (scale) {
            if (scale == -1) {
                if (currentScale == 0) {
                    console.info("Can not zoom out more!");
                } else {
                    if (--currentScale == 0) {
                        $("#Table").css("-webkit-transform", "");
                    } else {
                        $("#Table").css("-webkit-transform",
                            "scale(" + scaleLevels[currentScale] + ")");
                    }
                }
            } else if (scale == 1) {
                if (currentScale == scaleLevels.length) {
                    console.info("Can not zoom in more!");
                } else {
                    $("#Table").css("-webkit-transform",
                        "scale(" + scaleLevels[currentScale++ + scale] + ")");
                }
            } else {
                console.error("Invalid scaling provided!");
            }
        },

        // Joining data to table html elements.
        joinTable = function (tables, svgTable) {
            tables = svgTable
                .selectAll(".tableGroup")
                .data(function () {
                    return Spark.Visualizer.Model.Table.getTables();
                });
            return tables;
        },

        // Entering data and make a separate variable for it.
        enterTable = function(tables) {
            var tablesEntering = tables
                .enter();
            return tablesEntering;
        },

        // Adding table html elements to DOM.
        addTable = function(tablesEntering) {
            tablesEntering
                .append("g")
                .attr("id", function (d) {
                    return "tableGroup_" + d.tableIndex;
                })
                .attr(attributes.tableAttr.tableGroup)
                .append("rect")
                    .attr(attributes.tableAttr.tableRect)
                    .attr("width", function (d, i) {
                        return setTableRectWidth(d, i);
                    })
                    .attr("height", function (d, i) {
                        return setTableRectHeight(d, i);
                    });
        },

        // Positioning separately from the entered data.
        positionTable = function(tables) {
            tables
                .attr("transform", function (d) {
                    return tableTranslate(d);
                });
        },

        // Adding stage label data and appending stage label html elements.
        addStageLabel = function(tables) {
            tables.selectAll(".stageLabels")
                .data(function (d, i) {
                    return Object.keys(Spark.Visualizer.Model.Table.getTableIndexToStagePartMap()[i]);
                })
                .enter()
                .append("foreignObject")
                .attr(attributes.tableAttr.stageLabels)
                .attr("x", function (d, i) {
                    return getStageLabelX(d, i)
                })
                .attr("y", function (d, i) {
                    return getStageLabelY(d, i)
                })
                .append("xhtml:div")
                .attr(attributes.tableAttr.stageLabelsDiv)
                .append("span")
                .attr(attributes.tableAttr.stageLabelsSpan)
                .text(function (d) {
                    return "Stage " + d
                });
        },

        // Joining data of tasks inside each table.
        joinTasks = function(tables) {
            var $3Task = tables.selectAll(".tableTaskGroup")
                .data(function (d) {
                    return d.tasks;
                });
            return $3Task;
        },

        // Entering data and make a separate variable for it.
        enterTasks = function($3Task) {
            var $3TaskEntering = $3Task
                .enter()
                .append("g")
                .attr(attributes.tableAttr.tableTaskGroup);
            return $3TaskEntering;
        },

        // Adding task html elements to DOM.
        addTasks = function($3TaskEntering) {
            $3TaskEntering
                .append("rect")
                .attr(attributes.tableAttr.taskRect)
                .attr("id", function (d) {
                    return "tableTask" + d.ID;
                })
                .attr("width", params.tableParams.taskRectWidth)
                .attr("height", params.tableParams.taskRectHeight)
                .on("mouseover", highlightLink)
                .on("mouseout", function (d) {
                    return cancelHighlightLink(d);
                });
        },

        // Positioning separately from the entered data.
        positionTasks = function($3Task) {
            $3Task.selectAll(".task.rect")
                .attr("transform", function (d) {
                    return taskTranslate(d);
                });
        },

        // Adding task content in a html fraction using foreignObject for wrapping inside svg canvas.
        addTasksContent = function($3TaskEntering) {
            var $3TaskElementEntering =
                $3TaskEntering
                    .append("foreignObject")
                    .attr(attributes.tableAttr.foreignObject)
                    .append("xhtml:div")
                    .attr(attributes.graphAttr.taskContainer)
                    .append(function () {
                        return Spark.Utils.cloneNode("#Prototypes .Task.Table.Element");
                    })
                    .attr("id", function (d) {
                        return "tableTaskContainer" + d.ID;
                    })
                    .style("width", params.tableParams.taskRectWidth + "px")
                    .style("height", params.tableParams.taskRectHeight + "px")
                    .style("background-color", function (d) {
                        return Spark.Visualizer.Execution.View.Colorize.getColorForGroup(
                            d.executorId, "executor")
                    })
                    .style("border-color", function (d) {
                        return Spark.Visualizer.Execution.View.Colorize.getColorForGroup(
                            d.executorId, "executor")
                    });
            return $3TaskElementEntering;
        },

        // Positioning task content and classing finished elements.
        positionTasksContent = function($3Task) {
            $3Task.selectAll(".tableForeignobj")
                .attr("transform", function (d) {
                    return taskTranslate(d);
                });

            $3Task.selectAll(".tableTaskGroup .Task.Table.Element")
                .classed({
                    "Finished": function (d) {
                        return d.finished
                    }
                });
        },

        // Filling task contents, updating elapsed time since task launch.
        fillTasksContent = function($3TaskElementEntering, $3Task) {
            $3TaskElementEntering
                .select("DIV.Header SPAN.Stage SPAN:nth-child(2)").text(function (t) {
                    return t.stageId
                });
            $3TaskElementEntering
                .select("DIV.Header SPAN.Partition SPAN:nth-child(2)").text(function (t) {
                    return t.partitionId
                });
            $3TaskElementEntering
                .select("DIV.Header SPAN.Name").text(function (t) {
                    return t.operator
                });
            $3Task
                .selectAll(".tableTaskGroup .Task.Table.Element").select("SPAN.Runtime")
                .text(function (t) {
                    return Spark.Utils.getElapsedTimeHumanReadable(
                        t.launchTime,
                        (t.finished) ? t.finishTime : undefined)
                })
                .attr("data-elapsed", function (t) {
                    return Spark.Utils.getElapsedTime(
                            t.launchTime,
                            (t.finished) ? t.finishTime : undefined) / 1000
                });
            $3TaskElementEntering
                .select("DIV.Body LI.Prototype.Pipelined.Inner.RDD").remove();
            $3TaskElementEntering
                .select("DIV.Body UL").selectAll("LI")
                .data(function (t) {
                    return t.rddData
                })
                .enter()
                .append(function () {
                    return Spark.Utils.cloneNode(
                        "#Prototypes .Task.Table.Element div.Body li.Prototype.Pipelined.Inner.RDD")
                })
                .select(".Name").text(function (t) {
                    return t.scope.name
                });
        },

        // This is the main function for building up visualization elements of tables and tasks.
        drawTables = function() {
            tables = joinTable(tables, svgTable);
            var tablesEntering = enterTable(tables);
            addTable(tablesEntering);
            positionTable(tables);
            addStageLabel(tables);
            var $3Task = joinTasks(tables);
            var $3TaskEntering = enterTasks($3Task);
            addTasks($3TaskEntering);
            positionTasks($3Task);
            var $3TaskElementEntering = addTasksContent($3TaskEntering);
            positionTasksContent($3Task);
            fillTasksContent($3TaskElementEntering, $3Task);
        },

        // Joining links data.
        joinLinks = function() {
            var $3Links =
                svgTable
                    .selectAll(".tableLink.group")
                    .data(function () {
                        return Spark.Visualizer.Model.Table.getTableLinks()
                });
            return $3Links;
        },

        // Adding link html elements with style and class attributes.
        addLinks = function($3Links) {
            $3Links
                .enter()
                .append("g")
                .attr(attributes.tableAttr.linkGroup)
                .append("line")
                .attr(attributes.tableAttr.linkLine)
                .attr("id", function (d) {
                    return d.sourceTaskId + "-" + d.targetTaskId
                })
                .style("stroke-dasharray", function (d) {
                    var result = "";
                    if (d.linkType == "local") {
                        result = "10,5";
                    }
                    return result
                })
                .classed("Local", function (d) {
                    return d.linkType == "local"
                });
        },

        // Positioning link start and end points.
        positionLinks = function($3Links) {
            $3Links.selectAll("LINE")
                .attr("x1", function (d) {
                    return getLinkX1(d)
                })
                .attr("y1", function (d) {
                    return getLinkY1(d)
                })
                .attr("x2", function (d) {
                    return getLinkX2(d)
                })
                .attr("y2", function (d) {
                    return getLinkY2(d)
                });
        },

        // Setting link thickness.
        setLinkThickness = function($3Links) {
            $3Links
                .selectAll("LINE")
                .style("stroke-width", function (d) {
                    return thicknessScale(d.linkThickness)
                })
        },

        // This is the main function for building up visualization elements of links.
        drawLinks = function() {
            var $3Links = joinLinks(svgTable);
            addLinks($3Links);
            positionLinks($3Links);
            setLinkThickness($3Links);
        },

        // This is the main drawing function.
        redraw = function () {
            drawTables();
            drawLinks();
        };

    return {
        init: init,
        redraw: redraw,
        rescale: rescale
    }
}());