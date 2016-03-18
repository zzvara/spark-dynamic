Spark.Visualizer.Execution.Control = (function() {
    var currentView = Spark.Visualizer.Execution.TABLE_VIEW,
        isRunning = true,
        tableScale = 1,

        /**
         * Temporary store for task events, if visualizer has been frozen.
         * @type {Array}
         */
        eventBuffer = [],
        timerHandler,
        time = 0,
        reader,

        /**
         * @todo Rewrite comments!
         */
        processInput = function(taskEvent) {
            if(!isRunning) {
                eventBuffer.push(taskEvent);
                return;
            }

            if (taskEvent.event != "TaskLaunched" && taskEvent.event != "TaskFinished") {
                console.error("Not supported report type, while reading input: " +
                    taskEvent.type + "!");
                return;
            }

            if (taskEvent.event == "TaskLaunched") {
                Spark.Visualizer.Model.Table.addLaunchEvent(taskEvent);
                Spark.Visualizer.Model.Graph.addLaunchEvent(taskEvent);
            } else if (taskEvent.event == "TaskFinished") {
                Spark.Visualizer.Model.Table.addFinishEvent(taskEvent);
                Spark.Visualizer.Model.Graph.addFinishEvent(taskEvent);
            }

            Spark.Visualizer.Execution.View.Table.redraw();
            Spark.Visualizer.Execution.View.Graph.redraw();

            Spark.Visualizer.Execution.View.refreshStats({
                nNodes: Spark.Visualizer.Model.Graph.getNnodes(),
                nExecutors: Spark.Visualizer.Model.Graph.getNexecutors(),
                nRunningTasks: Spark.Visualizer.Model.Graph.getNrunningTasks(),
                nCompletedTasks: Spark.Visualizer.Model.Graph.getNCompletedTasks()
            });
        },

        startTimer = function() {
            timerHandler = window.setInterval(updateTimer, 1000);
        },

        stopTimer = function() {
            window.clearInterval(timerHandler);
        },

        updateTimer = function() {
            // Update global timer.
            d3.select(".Current.Time .Value")
                .text(Spark.Utils.timestampToHumanReadable(++time * 1000));
            // Also update task timers.
            $runtimes = $(".Task.Table.Element:not(.Finished) SPAN.Runtime");
            $runtimes.each(function() {
                elapsed = $(this).data("elapsed") + 1;
                $(this).data("elapsed", elapsed);
                $(this).text(Spark.Utils.elapsedTimeToHumanReadable(elapsed));
            });

            if($("BODY").hasClass("Auto")) {
                console.log("Autopiloting.");
                auto();
            }
        },

        auto = function() {
            $tables = $(".tableGroup").not(".Old");
            if($tables.length == 0) {
                return;
            }
            $tables.each(function() {
                this.classList.add("Old");
            });
            $table = $tables.first();
            /**
             * left: 220
             * top: 190
             */
            var moveToLeft = $table.offset().left - 220;
            var moveToTop = $table.offset().top - 190;

            currentLeft = $("#tableSvgContainer").offset().left;
            currentTop = $("#tableSvgContainer").offset().top;

            $("#tableSvgContainer").animate({
                left: currentLeft - moveToLeft + "px",
                top:currentTop - moveToTop + "px"
            }, 200);
        },

        init = function() {
            reader = Spark.Visualizer.Execution.Reader(processInput);
            startTimer();
            Spark.Visualizer.Execution.View.Graph.initGraphVis();
            Spark.Visualizer.Execution.View.Table.init();
            Spark.Visualizer.Execution.Control.Handlers();
        };

    return {
        /**
         * Rescale is support only for graph view.
         * @param {number} scale
         */
        rescale: function(scale) {
            Spark.Visualizer.Execution.View.Graph.rescaleGraph(scale);
            Spark.Visualizer.Execution.View.Graph.redraw();

            Spark.Visualizer.Execution.View.Table.rescale(scale);
        },
        isRunning: function() {
            return isRunning;
        },
        isTableView: function() {
            return currentView == Spark.Visualizer.Execution.TABLE_VIEW;
        },
        isGraphView: function() {
            return currentView == Spark.Visualizer.Execution.GRAPH_VIEW;
        },
        toGraphView: function() {
            currentView = Spark.Visualizer.Execution.GRAPH_VIEW;
        },
        toTableView: function() {
            currentView = Spark.Visualizer.Execution.GRAPH_VIEW;
        },
        updateGraphView: function() {
            Spark.Visualizer.Execution.View.Graph.redraw();
        },
        changeView: function() {
            if(this.isTableView()) {
                this.toGraphView();
            } else {
                this.toTableView();
            }
        },
        freeze: function() {
            isRunning = false;
            stopTimer();
        },
        unfreeze: function() {
            isRunning = true;
            $.each(eventBuffer, function(index, taskEvent) {
                processInput(taskEvent);
            });
            eventBuffer = [];
            startTimer();
        },
        init: init,
        auto: auto
    };
}());

$(function() {
    Spark.Visualizer.Execution.Control.init();
});

/**
 * Attaches handlers to control elements in the DOM.
 * Every user action is wired here.
 * @type {*|jQuery|HTMLElement}
 */
Spark.Visualizer.Execution.Control.Handlers = function() {
    control = Spark.Visualizer.Execution.Control;

    // we add event listeners to refreshBytePrefix button here. It keeps circulating over the prefixWholeNames and write the amount of data, according that.
    d3.select(".Wrapper.Settings .Edge.Scale").on("click", function() {
        var prefixWholeNames = ["bytes", "kilobytes", "megabytes", "gigabytes", "terrabytes"];
        Spark.Visualizer.Execution.View.Params.graphParams.bytePrefixId = (Spark.Visualizer.Execution.View.Params.graphParams.bytePrefixId + 1) % 5;
        var innerHtml = "<span>show network traffic in</span> " + prefixWholeNames[Spark.Visualizer.Execution.View.Params.graphParams.bytePrefixId];
        d3.select(".Wrapper.Settings button.Edge.Scale")
            .html(innerHtml);

        control.updateGraphView();
    });


    // Here we add an event listener on 'click' event to the Change View classed button in index.html.
    // The function will execute, when we click on the button. The function is quite simple. It switches the style attribute "dsplay" of the main svg canvases:
    // the graph canvas and table canvas. One will be display: none and one will be display block. So one will be visible while the other invisible.
    /**
     * @todo Rewrite view changer!
     */
    d3.select("nav.Main button").on("click", function () {
        control.changeView();

        var graphContainerDisplay = d3.select("div#graphSvgContainer").style("display");
        var tableContainerDisplay = d3.select("div#tableSvgContainer").style("display");
        d3.select("div#graphSvgContainer").style("display", tableContainerDisplay);
        d3.select("div#tableSvgContainer").style("display", graphContainerDisplay);

        var graphDisplay = d3.select("svg#Graph").style("display");
        var tableDisplay = d3.select("svg#Table").style("display");
        d3.select("svg#Graph").style("display", tableDisplay);
        d3.select("svg#Table").style("display", graphDisplay);

        // Display none to menu elements in table view mode.
        if(control.isTableView()) {
            //d3.select(".Wrapper.Settings .Runner").style("display", "none");
            d3.select(".Wrapper.Settings .In").style("display", "none");
            d3.select(".Wrapper.Settings .Out").style("display", "none");
            //d3.select(".Wrapper.Settings .Edge.Scale").style("display", "none");

            //d3.select("div.Wrapper.Runtime").style("display", "none");
        } else {
            //d3.select(".Wrapper.Settings .Runner").style("display", "block");
            d3.select(".Wrapper.Settings .In").style("display", "block");
            d3.select(".Wrapper.Settings .Out").style("display", "block");
            //d3.select(".Wrapper.Settings .Edge.Scale").style("display", "block");

            //d3.select("div.Wrapper.Runtime").style("display", "block");
        }
    });

    d3.select(".Wrapper.Settings .Runner").on("click", function() {
        if(control.isRunning()) {
            control.freeze();
            d3.select(".Wrapper.Settings .Stop")
                .text("start")
                .classed("Stop", false)
                .classed("Start", true);
            d3.select("BODY").classed("Stopped", true);
        } else {
            control.unfreeze();
            d3.select(".Wrapper.Settings .Start")
                .text("stop")
                .classed("Start", false)
                .classed("Stop", true);
            d3.select("BODY").classed("Stopped", false);
        }
    });

    d3.select(".Wrapper.Settings .In").on("click", function() {
        control.rescale(1);
    });

    d3.select(".Wrapper.Settings .Out").on("click", function() {
        control.rescale(-1);
    });

    $("#tableSvgContainer").draggable({
        cursor: "move",
        drag: function(event, ui) {

        }
    });

    $("BODY").on("click", ".tableTaskGroup", function() {
        $(this).find(".Body").toggleClass("Open");
    });

    $(document).on("contextmenu", ".tableTaskGroup", function(e){
        var $histogram = $(this).find(".Histogram");
        if($histogram.hasClass("Show")) {
            $histogram.removeClass("Show");
            return false;
        }

        if($histogram.hasClass("Loaded")) {
            $histogram.addClass("Show");
            return false;
        }

        /**
         * Lazy loading.
         */
        var histogramData = d3.select(this).data()[0].histogram;

        if(typeof histogramData == "undefined" || histogramData.length == 0) {
            console.info("No histogram data for the given task.");
            return false;
        }

        max = histogramData[histogramData.length - 1][1];

        $.each(histogramData, function(i, e) {
            $bar = $( ".Bar.Prototype" ).clone().appendTo($histogram);
            $bar.removeClass("Prototype");
            $bar.find(".Key")
                .html(e[0])
                .css("background-color", Spark.Utils.convertHex("#0F5959", 100 - (i * 10)))
                .css("width", 160 * (e[1] / max));
            $bar.find(".Value")
                .html(e[1])
        });

        $histogram.addClass("Loaded");
        $histogram.addClass("Show");

        return false;
    });

    $("#Settings").find(".Edges").on("click", function() {
        $button = $(this);

        $button.toggleClass("Remote");
        $("#tableSvgContainer").toggleClass("Remote");

        nextText = $button.data("next-text");
        $button.data("next-text", $button.text());
        $button.text(nextText);
    });

    $("#Settings").find(".Auto").on("click", function() {
        $button = $(this);

        $button.toggleClass("On");
        $("BODY").toggleClass("Auto");

        nextText = $button.data("next-text");
        $button.data("next-text", $button.text());
        $button.text(nextText);
    });

    $("#Table").on("mousewheel", function(e) {
        if(e.originalEvent.wheelDelta / 120 > 0) {
            control.rescale(1);
        } else {
            control.rescale(-1);
        }
    });
};
