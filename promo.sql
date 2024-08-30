max_over_time(sum(
  node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate{cluster="", namespace="prd-butler-mobile"}
* on(namespace,pod)
  group_left(workload, workload_type) namespace_workload_pod:kube_pod_owner:relabel{cluster="", namespace="prd-butler-mobile", workload_type="deployment"}
) [1w:]) by (workload, workload_type)
