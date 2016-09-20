package org.apache.spark.repartitioning

import org.apache.spark.rpc.RpcEndpointRef

class WorkerReference(
  executorID: String,
  reference: RpcEndpointRef)
extends core.WorkerReference[RpcEndpointRef](executorID, reference) { }
