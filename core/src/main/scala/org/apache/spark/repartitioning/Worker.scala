package org.apache.spark.repartitioning

import org.apache.spark.rpc.RpcEndpointRef

class Worker(val executorID: String,
             val reference: RpcEndpointRef)
