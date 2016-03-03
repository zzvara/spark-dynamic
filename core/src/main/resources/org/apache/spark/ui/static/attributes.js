Spark.Visualizer.Execution.View.Attributes = {

    /*
     An object for holding different svg attributes. i. e. the radius of executor circle, or the width/height of the rectangle tasks in table view.
     Defining the classes of the svg elements here as well.
     */

    graphAttr: {
        svgContainer: {id: "graphSvgContainer", style: "height:" + window.innerHeight + "px;width:" + window.innerWidth + "px;overflow-x:scroll;overflow-y:scroll"},
        //svgGraph: {id: "Graph", class: "svg", viewBox: "0 0 " + window.innerWidth + " " + window.innerHeight, preserveAspectRatio: "xMidYMid meet", "data-scale": 0},
        svgGraph: {id: "Graph", class: "svg", width: 5000, height: 5000, preserveAspectRatio: "xMidYMid meet", "data-scale": 0},
        marker: {id: "graphMarker", class: "marker", viewBox: "0 -5 10 10", refX: 5, refY: 0, markerWidth: 6, markerHeight: 6, orient: "auto"},
        markerPath: {class: "graph marker path", d : "M 0, -2.5 L 5, -0.1 L 5, 0.1 L 0, 2.5 L 0, -2.5"},
        node: {class: "node"},
        nodeCircle: {class: "node circle", r: 90},
        foreignObject: {class: "task foreignobj", width: "90px", height: "90px"},
        nodeInfoBox: {class: "node info"},
        taskContainer: {class: "task container"},
        taskList: {class: "task list"},
        link: {class: "link"},
        linkPath: {class: "link path"},
        linkTextGroup: {class: "link group"},
        linkTextRect: {class: "link rect"},
        linkText: {class: "link text", "dy": "-0.15em"},
        hullGroups: {class: "hull group"},
        hullInfoGroup: {class: "hull info"},
        hullInfoText: {class: "hull info text"},
        hullInfoRect: {class: "hull info rect"}
    },

    tableAttr: {
        svgContainer: {id: "tableSvgContainer", style: "height:" + window.innerHeight + "px;width:" + window.innerWidth + "px;"},
        //svgTable: {id: "Table", class: "svg", viewBox: "0 0 " + 5000 + " " + 5000, preserveAspectRatio: "xMidYMid meet", "data-scale": 0},
        svgTable: {id: "Table", class: "svg", width: 5000, height: 5000, preserveAspectRatio: "xMidYMid meet", "data-scale": 0},
        marker: {id: "tableMarker", class: "marker", viewBox: "0 -5 10 10", refX: 20, refY: 0, markerWidth: 6, markerHeight: 6, orient: "auto"},
        markerPath: {class: "marker path", d : "M 0, -2.5 L 5, -0.1 L 5, 0.1 L 0, 2.5 L 0, -2.5"},
        tableGroup: {class: "tableGroup"},
        tableRect: {class: "tableRect"},
        tableTaskGroup: {class: "tableTaskGroup"},
        taskRect: {class: "task rect"},
        foreignObject: {class: "tableForeignobj", width: "36px", height: "26px"},
        linkGroup: {class: "tableLink group"},
        linkLine: {class: "tableLink line"},
        linkLabelGroup: {class: "tableLinkLabel group"},
        linkLabelRect: {class: "tableLinkLabel rect"},
        linkLabelText: {class: "tableLinkLabel text"},
        stageLabelsDiv: {class: "stageLabelsDiv"},
        stageLabels: {class: "stageLabels"},
        stageLabelsSpan: {class: "stageLabelsSpan"}
    }
}