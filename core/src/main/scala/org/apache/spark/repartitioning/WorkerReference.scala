package org.apache.spark.repartitioning

import org.apache.spark.rpc.RpcEndpointRef

class WorkerReference(
  executorID: String,
  reference: RpcEndpointRef)
extends hu.sztaki.drc.WorkerReference[RpcEndpointRef](executorID, reference)
