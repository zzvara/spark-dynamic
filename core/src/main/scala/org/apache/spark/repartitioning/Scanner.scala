package org.apache.spark.repartitioning

import org.apache.spark.TaskContext
import org.apache.spark.executor.TaskMetrics

abstract class Scanner(totalSlots: Int)
extends core.Scanner[TaskContext, TaskMetrics](totalSlots) {

}
