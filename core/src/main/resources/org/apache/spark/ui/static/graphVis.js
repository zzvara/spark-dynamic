Spark.Visualizer.Execution.View.Graph = (function() {
  var
    svgContainer,
    svgGraph,
    node,
    link,
    hullg,
    hullGroups,
    hull,
    hullInfoGroup,
    attributes = Spark.Visualizer.Execution.View.Attributes,
    params = Spark.Visualizer.Execution.View.Params,
    force = Spark.Visualizer.Execution.View.Force,

    init = function() {
      svgContainer = d3.select("body").append("div")
          .attr(attributes.graphAttr.svgContainer);

      svgGraph = svgContainer.append("svg")
          .attr(attributes.graphAttr.svgGraph);

      svgGraph.append("defs").append("marker")
          .attr(attributes.graphAttr.marker)
          .append("path")
          .attr(attributes.graphAttr.markerPath);

      hullg = svgGraph.append("g");

      hullGroups = hullg.selectAll("g.hull.group");
      hull = hullg.selectAll("path.hull");
      hullInfoGroup = hullg.selectAll(".hull.info");
    },

    thicknessScale = function(value) {
      var scale = d3.scale.log()
        .domain([1, d3.max(Spark.Visualizer.Model.Graph.getBytesList())])
        .range([params.graphParams.minThickness, params.graphParams.maxThickness]);
      return scale(value);
    },

    getReadableSize = function(sizeInBytes) {
      var prefixNames = [' B', ' kB', ' MB', ' GB', ' TB'];
      var denominators = [1, 1024, Math.pow(1024, 2), Math.pow(1024, 3), Math.pow(1024, 4)];
      sizeInBytes = sizeInBytes / denominators[params.graphParams.bytePrefixId];
      return sizeInBytes.toFixed(1) + prefixNames[params.graphParams.bytePrefixId];
    },

    replaceSpecChars = function(str) {
      if (str == null) {
        result = null;
      } else {
        result = "" + str;
        result = result.replace(/[&\/\\#,+()$~%.'":*?<>{}]/g, '_');
      }
      return result;
    },

    drawCluster = function(d) {
      var curve = d3.svg.line()
          .interpolate("cardinal-closed")
          .tension(.85);

      return curve(d.path);
    },

    countInfoBoxXY = function(pointSet) {
      var result = {};
      var pointSetX = [];
      var pointSetY = [];
      for (var j = 0; j < pointSet.length; ++j) {
        pointSetX.push(pointSet[j][0]);
        pointSetY.push(pointSet[j][1]);
      }
      result.x = (d3.min(pointSetX) + d3.max(pointSetX)) / 2;
      result.y = d3.min(pointSetY) - params.graphParams.hullInfoSpacing;
      return result;
    },

    createHulls = function(nodes, offset) {
      var hulls = {};

      // create point sets
      for (var k = 0; k < nodes.length; ++k) {
        var currGroup = nodes[k].host_id;
        if (hulls[currGroup] == null) {
          hulls[currGroup] = {"host": nodes[k].host, "pointSet": [], "infoX": 0, "infoY": 0};
        }
        hulls[currGroup].pointSet.push([nodes[k].x - offset, nodes[k].y - offset]);
        hulls[currGroup].pointSet.push([nodes[k].x - offset, nodes[k].y + offset]);
        hulls[currGroup].pointSet.push([nodes[k].x + offset, nodes[k].y - offset]);
        hulls[currGroup].pointSet.push([nodes[k].x + offset, nodes[k].y + offset]);
      }

      // count info box's x,y coordinate
      for (i in hulls) {
        hulls[i].infoX = countInfoBoxXY(hulls[i].pointSet).x;
        hulls[i].infoY = countInfoBoxXY(hulls[i].pointSet).y;
      }

      // create convex hulls
      var hullset = [];
      for (i in hulls) {
        hullset.push({host: hulls[i].host, host_id: i, path: d3.geom.hull(hulls[i].pointSet), infoX: hulls[i].infoX, infoY: hulls[i].infoY});
      }

      return hullset;
    },

    filterVisibleLinks = function(links) {
      var result = [];
      for (var i = 0; i < links.length; ++i) {
        if (links[i].visibility == "visible") {
          result.push(links[i]);
        };
      }
      return result;
    },

    setStrokeWidth = function(linkId, width) {
      var selectedLink = svgGraph.select("#linkID" + replaceSpecChars(linkId));
      selectedLink.transition()
        .duration(300)
        .style("stroke-width", width);
    };

    getEmptyClass = function(d) {
      if (d.currentTasks.length > 0) {
        return false;
      } else {
        return true;
      }
    },

    rescaleGraph = function(scale) {
        var scaleLevels = [15, 25, 35, 50, 90];
        if (scale == -1) {
            if (params.graphParams.zoomID > 0) {
              params.graphParams.zoomID--;
              params.graphParams.changeParams(scaleLevels[params.graphParams.zoomID]);
            } else {
                console.info("Can not zoom out more!");
            }
        } else if (scale == 1) {
            if (params.graphParams.zoomID < 4) {
              params.graphParams.zoomID++;
              params.graphParams.changeParams(scaleLevels[params.graphParams.zoomID]);
            } else {
                console.info("Can not zoom in more!");
            }
        } else {
            console.error("Invalid scaling provided!");
        }
    },

    clear = function() {
      svgGraph.selectAll("rect").remove();

      svgGraph.selectAll(".hull.info.text").remove();
      svgGraph.selectAll("path.hull").remove();
      svgGraph.selectAll(".hull.info").remove();
    },

    drawNodes = function() {
      node = svgGraph
          .selectAll(".node")
          .data(force.force.nodes(), function(d) { return d.name;});

      var nodeEnteringGroup = node.enter().append("g")
        .attr(attributes.graphAttr.node)
        .classed("Empty", function(d) {return getEmptyClass(d);})
        .attr('id', function(d) { return replaceSpecChars(d.name);})
        .call(force.force.drag);

      var nodeEnteringCircle = nodeEnteringGroup
        .append("circle")
          .attr(attributes.graphAttr.nodeCircle)
          .classed("Empty", function(d) {return getEmptyClass(d);});

      var foreignObj = nodeEnteringGroup.append("foreignObject")
        .attr(attributes.graphAttr.foreignObject);

      var taskContainer = foreignObj.append("xhtml:div")
        .attr(attributes.graphAttr.taskContainer);

      var taskList = taskContainer.append("ul")
        .attr(attributes.graphAttr.taskList);

      var executors = Spark.Visualizer.Model.Graph.getExecutors();
      for (var i = 0; i < executors.length; ++i) {
        var taskListLi = svgGraph.select("#" + replaceSpecChars(executors[i].name) + " ul").selectAll("li")
          .data(function(d) { return d.currentTasks; })
          .enter()
          .append(function() {return Spark.Utils.cloneNode("#Prototypes .Task.Graph.Element");})
          .attr("id", function(d) {return replaceSpecChars("taskid" + d.task_id);})
          .classed("Finished", function(d) {return d.finished;});
      }

      for (var i = 0; i < executors.length; ++i) {
        for (var j = 0; j < executors[i].currentTasks.length; ++j) {
          var currentTask = executors[i].currentTasks[j];
          var currentTaskList = d3.select("#" + replaceSpecChars("taskid" + currentTask.task_id));
          currentTaskList.select(".Body .Taskid").text(currentTask.task_id);
          currentTaskList.select(".Body .RDDid").text(currentTask.rddId);
          currentTaskList.select(".Header .Stage span:nth-child(2)").text(currentTask.stage);
          currentTaskList.select(".Header .Partition span:nth-child(2)").text(currentTask.partition);
          currentTaskList.select(".Header .Name").text(currentTask.name);
        }
      }
    },

    drawLinks = function() {
      link = svgGraph
          .selectAll(".link")
          .data(filterVisibleLinks(force.force.links()), function(d) { return d.name;});

      var linkEnteringGroup = link.enter().append("g")
        .attr(attributes.graphAttr.link);

      linkEnteringGroup
        .append("path")
        .attr(attributes.graphAttr.linkPath)
        .attr('id', function(d) { return "linkID" + replaceSpecChars(d.name);});

      var linkTextGroup = linkEnteringGroup.append("g").attr(attributes.graphAttr.linkTextGroup);

      linkTextGroup.append("rect")
        .attr(attributes.graphAttr.linkTextRect);

      var linkText = linkTextGroup.append("text")
        .attr(attributes.graphAttr.linkText)
        .attr("text-anchor", "middle");

      var linkTextPath = linkText.append("textPath")
        .attr("class", "textPath")
        .attr("xlink:href", function(d) { return "#linkID" + replaceSpecChars(d.name);})
        .attr("startOffset", "50%");

      d3.selectAll(".link.path")
        .style("stroke-width", function(d) { return thicknessScale(d.linkThickness);});

      d3.selectAll(".textPath")
        .text(function(d) {return getReadableSize(d.linkThickness);});
    },

    drawHulls = function() {
      hullGroups = hullGroups.data(createHulls(force.force.nodes(), params.graphParams.hullOffset));

      hullGroups.enter().append("g")
        .attr(attributes.graphAttr.hullGroups);

      hull = hullGroups.append("path")
        .attr("class", "hull");

      hull.attr("d", drawCluster)
        .attr("host_id", function(d) { return d.host_id; })
        .style("fill", function(d) { return d3.scale.category20()(d.host_id); })
        .style("fill-opacity", params.graphParams.hullOpacity);


      hullInfoGroup = hullGroups.append("g")
        .attr(attributes.graphAttr.hullInfoGroup);

      hullInfoGroup.append("rect")
        .attr(attributes.graphAttr.hullInfoRect)
        .style("fill", function(d) { return d3.scale.category20()(d.host_id); });

      var hullInfoText = hullInfoGroup.append("text")
        .attr(attributes.graphAttr.hullInfoText)
        .attr("text-anchor", "middle")
        .text(function(d) {return d.host;});

      d3.selectAll(".hull.info.rect").attr("width", function(d, i) {
          return hullInfoText[0][i].getBBox().width;
      }).attr("height", function(d, i) {
          return hullInfoText[0][i].getBBox().height;
      });

      hullGroups.exit().remove();
    },

    redraw = function() {
      clear();
      drawNodes();
      drawLinks();
      drawHulls();
      force.force.start();
    },

    makeLinkPath = function(d) {
      var dx = d.target.x - d.source.x;
      var dy =  d.target.y - d.source.y;
      var dist = Math.sqrt(dx * dx + dy * dy);
      var r = attributes.graphAttr.nodeCircle.r;
      var lambda = r / dist;
      var x1 = d.source.x + lambda * dx;
      var y1 = d.source.y + lambda * dy;
      var x2 = d.target.x - lambda * dx;
      var y2 = d.target.y - lambda * dy;

      return "M" + x1 + "," + y1 + "A" + dist + "," + dist + " 0 0,1 " + x2 + "," + y2;
    },

    collide = function(alpha) {
      var quadtree = d3.geom.quadtree(Spark.Visualizer.Model.Graph.getExecutors());
      return function(d) {
        var rb = 2 * attributes.graphAttr.nodeCircle.r + params.graphParams.nodePadding,
            nx1 = d.x - rb,
            nx2 = d.x + rb,
            ny1 = d.y - rb,
            ny2 = d.y + rb;
        quadtree.visit(function(quad, x1, y1, x2, y2) {
          if (quad.point && (quad.point !== d)) {
            var x = d.x - quad.point.x,
                y = d.y - quad.point.y,
                l = Math.sqrt(x * x + y * y);
            if (l < rb) {
              l = (l - rb) / l * alpha;
              d.x -= x *= l;
              d.y -= y *= l;
              quad.point.x += x;
              quad.point.y += y;
            };
          }
          return x1 > nx2 || x2 < nx1 || y1 > ny2 || y2 < ny1;
        });
      };
    },

    updateOnTick = function() {

      d3.selectAll("path.hull").data(createHulls(force.force.nodes(), params.graphParams.hullOffset))
        .attr("d", drawCluster);

      d3.selectAll(".hull.info.text").data(createHulls(force.force.nodes(), params.graphParams.hullOffset));

      d3.selectAll(".hull.info.text").attr("x", function(d) {
          return d.infoX;
      })
      .attr("y", function(d) {
          return d.infoY;
      });

      var hullTexts = d3.selectAll(".hull.info.text");
      d3.selectAll(".hull.info.rect").attr("x", function(d, i) {
          return hullTexts[0][i].getBBox().x;
      }).attr("y", function(d, i) {
          return hullTexts[0][i].getBBox().y;
      });

      d3.selectAll("circle").attr("cx", function (d) {
          return d.x;
      }).attr("cy", function (d) {
          return d.y;
      });

      d3.selectAll(".task.foreignobj").attr("x", function (d) {
          return d.x - params.graphParams.foreignObjDx;
      }).attr("y", function (d) {
          return d.y - params.graphParams.foreignObjDy;
      });

      d3.selectAll(".link.path")
        .attr("d", makeLinkPath);

      d3.selectAll("circle").each(collide(0.5));

    };

  return {
    initGraphVis: init,
    redraw: redraw,
    updateOnTick: updateOnTick,
    setStrokeWidth: setStrokeWidth,
    rescaleGraph: rescaleGraph
  }
}());
