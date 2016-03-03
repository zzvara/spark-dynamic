Spark.Visualizer.Execution.View.Params = {

  // An object for holding different global parameters. i. e. the inputFile, or the distance of the graph links.

    graphParams: {
      charge: -300,
      friction: 0.9,
      gravity: 0.02,
      linkDistInv: 250,
      linkDistVis: 450,
      hullOpacity: 0.3,
      hullOffset: 110,
      hullInfoSpacing: 15,
      minThickness: 1,
      maxThickness: 6,
      nodePadding: 5,
      foreignObjDx: 17,
      foreignObjDy: 17,
      bytePrefixId: 1,
      zoomID: 4,

      // This can change the size of the svg graph elements based on the radius of executor circle.
      changeParams: function (execCircleR) {
        this.hullOffset = 1.3 * execCircleR;

        Spark.Visualizer.Execution.View.Attributes.graphAttr.nodeCircle.r = execCircleR;

        this.minThickness = 1;
        this.maxThickness = 0.1 * execCircleR;

        d3.selectAll(".link .text")
            .style("font", 0.5 * execCircleR + "px helvetica");

        Spark.Visualizer.Execution.View.Attributes.graphAttr.foreignObject.width = execCircleR;
        Spark.Visualizer.Execution.View.Attributes.graphAttr.foreignObject.height = execCircleR;

        d3.selectAll(".task .container")
            .style("width", 0.9 * execCircleR + "px")
            .style("height", 0.9 * execCircleR + "px");

        this.foreignObjDx = 0.19 * execCircleR;
        this.foreignObjDy = 0.19 * execCircleR;
        this.linkDistInv = 3 * execCircleR;
        this.linkDistVis = 6 * execCircleR;
      }
    },
  
    tableParams: {
        taskOffSetX: 70,
        taskOffSetY: 16,
        taskRectWidth: 160,
        taskRectHeight: 60,
        minThickness: 1,
        maxThickness: 3,
        tableTaskScale: 80,
        initialTransX: 220,
        initialTransY: 200,
        linkLabelOffsetX: 10,
        tableOffSetX: 40,
        tableOffSetY: 40
    }
};