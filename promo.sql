sum(
  node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate{cluster="", namespace="prd-butler-mobile"}
* on(namespace,pod)
  group_left(workload, workload_type) namespace_workload_pod:kube_pod_owner:relabel{cluster="", namespace="prd-butler-mobile", workload_type="deployment"}
) by (workload, workload_type)
/sum(
  kube_pod_container_resource_limits{job="kube-state-metrics", cluster="", namespace="prd-butler-mobile", resource="cpu"}
* on(namespace,pod)
  group_left(workload, workload_type) namespace_workload_pod:kube_pod_owner:relabel{cluster="", namespace="prd-butler-mobile", workload_type="deployment"}
) by (workload, workload_type)
