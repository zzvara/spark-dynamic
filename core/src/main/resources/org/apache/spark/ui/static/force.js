Spark.Visualizer.Execution.View.Force = {

    force: d3.layout.force()
      .nodes(Spark.Visualizer.Model.Graph.getExecutors())
      .links(Spark.Visualizer.Model.Graph.getLinks())
      .charge(Spark.Visualizer.Execution.View.Params.graphParams.charge)
      .friction(Spark.Visualizer.Execution.View.Params.graphParams.friction)
      .gravity(Spark.Visualizer.Execution.View.Params.graphParams.gravity)
      .linkDistance(function(link) {
        if (link.source.host_id == link.target.host_id) {
          return Spark.Visualizer.Execution.View.Params.graphParams.linkDistInv;
        } else {
          return Spark.Visualizer.Execution.View.Params.graphParams.linkDistVis;
        }
      })
      .size([window.innerWidth, window.innerHeight])
      .on("tick", function() {return Spark.Visualizer.Execution.View.Graph.updateOnTick();})

}