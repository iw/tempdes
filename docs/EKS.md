# Temporal on EKS: how clients reach the frontend

This guide covers how SDK workers and clients should connect to the Temporal frontend on Amazon
EKS. It shows the choice as tempdes simulates it, then gives a setup for each option. Only the
frontend is reached through a Kubernetes Service. History, matching and worker pods find each
other through Temporal's membership ring.

## Why the connection model matters

An SDK process sends all its calls over one long-lived HTTP/2 connection. A ClusterIP Service
(kube-proxy) and an NLB balance connections, not requests. So each process sends everything to
one frontend pod. It stays there until the frontend closes the connection with GOAWAY at
`frontend.keepAliveMaxConnectionAge` (default 5 minutes, ±10% jitter). The client then reconnects
to another pod chosen at random.

The frontend's limits are per pod: `frontend.rps`, `frontend.namespaceRPS` and
`frontend.namespaceCount`. Worker polls are the lowest-priority calls. So the pod that happens to
hold the busiest connections rejects polls while the other pods still have room. Rejected pollers
back off for 1–10 s, and tasks wait in matching although workers are idle.

This is `examples/scenarios/frontend-lb.yaml`: 270 workflow starts/s on 3 frontends with the
default limits of 2,400 requests/s per pod. Each mode ran with five seeds; the seed decides
which connections land on which pod.

| `client_lb` | Seeds with rejections | Rejections/s, mean / max | Busiest frontend vs mean | Workflow-task wait p99, worst | End-to-end p99, worst |
|---|---|---|---|---|---|
| `pinned` (ClusterIP / NLB) | 5 of 5 | 41 / 118 | 1.49× | 4.6 s | 8.7 s |
| `round_robin` (client-side) | 0 of 5 | 0 | 1.00× | 0.03 s | 2.8 s |
| `proxy` (ALB / mesh) | 0 of 5 | 0 | 1.00× | 0.03 s | 2.8 s |

Frontend CPU stayed at 16% or below in every run. The per-pod limits bind long before CPU does,
so the uneven load is the whole problem.

### Scaling out

An autoscaler can add frontend pods, but clients only send them traffic once they reconnect
(`pinned`) or re-resolve DNS (`round_robin`). Both are driven by the max connection age.
`examples/scenarios/frontend-scale-out.yaml` scales from 3 to 6 frontends. The table shows the new
pods' share of requests in the two minutes after the scale-out. An even spread would be 50%.

| `client_lb` | Max connection age 5m (default) | Max connection age 1m |
|---|---|---|
| `pinned` | 0%, polls still rejected (61/s) | 48%, still rejected (14/s): random placement stays uneven |
| `round_robin` | 0%, the old pods stay evenly loaded | 39%, no rejections |
| `proxy` (15 s to register a new pod) | 44%, no rejections | 44%, no rejections |

With the default 5-minute age, a scale-out takes effect for clients only after about 5 minutes.
With `round_robin`, a 1-minute age brings it down to about a minute.

## Recommended setups

### Clients in the same cluster: gRPC client-side round robin

**1. Create a headless Service that publishes only Ready frontend pods.** The
`temporalio/helm-charts` chart already creates `<release>-frontend-headless`. That Service sets
`publishNotReadyAddresses: true`, which membership needs, so don't point clients at it.

```yaml
apiVersion: v1
kind: Service
metadata:
  name: temporal-frontend-lb
  namespace: temporal
spec:
  clusterIP: None                  # headless: DNS returns one A record per Ready pod
  publishNotReadyAddresses: false
  selector:                        # the chart's frontend pod labels
    app.kubernetes.io/name: temporal
    app.kubernetes.io/instance: temporal
    app.kubernetes.io/component: frontend
  ports:
    - name: grpc-rpc
      port: 7233
      targetPort: 7233
```

**2. Point the SDKs at it with the `round_robin` policy.**

The Go SDK already requests `round_robin` in its default gRPC service config, and dials with
`grpc.NewClient`. So it only needs a target that resolves to every pod:

```go
c, err := client.Dial(client.Options{
	HostPort:  "dns:///temporal-frontend-lb.temporal.svc.cluster.local:7233",
	Namespace: "orders",
})
```

grpc-java defaults to `pick_first`, so the Java SDK needs the policy set explicitly:

```java
WorkflowServiceStubs service = WorkflowServiceStubs.newServiceStubs(
    WorkflowServiceStubsOptions.newBuilder()
        .setTarget("dns:///temporal-frontend-lb.temporal.svc.cluster.local:7233")
        .setChannelInitializer(channel -> channel.defaultLoadBalancingPolicy("round_robin"))
        .build());
```

The TypeScript, Python, .NET and Ruby SDKs are built on the Rust Core SDK. At the time of writing,
it has no documented client-side load-balancing option. Check your SDK's release notes, and use
the proxy setup otherwise.

**3. Set the frontend's connection lifetime and shutdown behaviour** in dynamic config:

```yaml
frontend.keepAliveMaxConnectionAge:
  - value: 1m    # clients re-resolve DNS (and pinned clients reconnect) at least this often
frontend.shutdownFailHealthCheckDuration:
  - value: 15s   # report NOT_SERVING first; with the gRPC probe below, the pod leaves DNS before it drains
frontend.shutdownDrainDuration:
  - value: 30s   # then finish in-flight requests
```

- **Max age.** A shorter max age means more reconnects, and so more TLS handshakes. One per
  connection per minute is cheap.
- **Grace period.** `frontend.keepAliveMaxConnectionAgeGrace` (default 70 s) lets in-flight long
  polls finish after GOAWAY.
- **Termination grace.** Give frontend pods a `terminationGracePeriodSeconds` longer than the two
  shutdown durations combined, for example 60 s.

**4. Use a gRPC readiness probe.** The frontend's health service reports NOT_SERVING as soon as
shutdown starts. So a gRPC probe removes the pod from DNS before it stops serving. In the chart:

```yaml
server:
  frontend:
    readinessProbe:
      grpc:
        port: 7233
        service: temporal.api.workflowservice.v1.WorkflowService
      periodSeconds: 5
    terminationGracePeriodSeconds: 60
```

kubelet's gRPC probes can't use TLS. If the frontend's gRPC port requires TLS, keep the chart's
`tcpSocket` probe.

**Trade-offs:**
- **More connections.** Every process connects to every frontend pod.
- **Cross-AZ traffic.** Requests spread across availability zones, and inter-AZ data transfer is
  billed. Topology-aware routing applies to ClusterIP Services, not to headless DNS.
- **Pod discovery.** New pods are found only when a connection is replaced, as the scale-out
  results show.

### Clients outside the cluster: an ALB with a gRPC target group

An NLB balances connections, so it behaves like `pinned`. That is fine with many client
processes, and uneven with few. An ALB balances each request over the frontend pods. With the
AWS Load Balancer Controller:

```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: temporal-frontend
  namespace: temporal
  annotations:
    alb.ingress.kubernetes.io/scheme: internal
    alb.ingress.kubernetes.io/target-type: ip                    # register pod IPs
    alb.ingress.kubernetes.io/backend-protocol-version: GRPC
    alb.ingress.kubernetes.io/listen-ports: '[{"HTTPS": 443}]'  # gRPC needs an HTTPS listener
    alb.ingress.kubernetes.io/certificate-arn: <acm-certificate-arn>
    alb.ingress.kubernetes.io/healthcheck-path: /grpc.health.v1.Health/Check
    alb.ingress.kubernetes.io/success-codes: "0"
    # long polls wait up to ~70 s with no data; the ALB default idle timeout is 60 s
    alb.ingress.kubernetes.io/load-balancer-attributes: idle_timeout.timeout_seconds=120
    alb.ingress.kubernetes.io/target-group-attributes: deregistration_delay.timeout_seconds=90
spec:
  ingressClassName: alb
  rules:
    - host: temporal.example.internal
      http:
        paths:
          - path: /
            pathType: Prefix
            backend:
              service:
                name: temporal-frontend
                port:
                  number: 7233
```

Clients then connect to `temporal.example.internal:443` with TLS.

- **Latency.** The ALB adds a hop: tempdes defaults `proxy_latency` to 1 ms.
- **Registration delay.** A new pod waits for registration and health checks before it gets
  traffic (`proxy_discovery`, default 15 s).
- **Health checks.** The gRPC health check succeeds while the frontend is still NOT_SERVING
  during shutdown, because the check only looks at the gRPC status code. Draining therefore
  relies on the pod leaving the endpoints and on the deregistration delay.

A service mesh such as Linkerd or Istio also balances per request, both inside and across
clusters. Make sure the proxies' request timeouts exceed the ~70 s long poll.

### Sizing the per-pod limits

- **Even spreading.** With `round_robin` or `proxy`, each frontend carries about the total
  divided by the number of pods. Size `frontend.rps` and `frontend.namespaceRPS` with headroom
  over that; tempdes warns at 70% of a limit.
- **Pinned connections.** With `pinned`, the busiest pod ran up to 1.49× the mean in the runs
  above. Size for the busiest pod, or change mode.
- **Global limits.** `frontend.globalNamespaceRPS` divides a cluster budget by the number of
  frontends. It doesn't follow where the load is, so it pairs badly with pinned connections.

## Trying it with tempdes

```bash
# the three modes side by side, at 3 and 4 frontends
tempdes sweep examples/scenarios/frontend-lb.yaml --rows frontend=3,4 \
    --cols client_lb=pinned,round_robin,proxy

# a 3 -> 6 frontend scale-out, with the default and a 1-minute max connection age
tempdes sweep examples/scenarios/frontend-scale-out.yaml --rows frontend=3 \
    --cols client_lb=pinned,round_robin,proxy --cols frontend.keepAliveMaxConnectionAge=5m,1m

# any scenario, one mode
tempdes run my-cluster.yaml --client-lb round_robin
```

In a scenario:

```yaml
cluster:
  network:
    client_lb: round_robin   # pinned (default) | round_robin | proxy
    proxy_latency: 1ms       # proxy only
    proxy_discovery: 15s     # proxy only
```

What the model does:

* **`pinned`.** One connection per process, placed on a random live frontend. It moves at the max
  connection age, or when its pod goes away.
* **`round_robin`.** This follows grpc-go and grpc-java:
  * one subchannel per resolved pod;
  * each call goes to the next ready subchannel;
  * the picker restarts at a random position after each re-resolution;
  * DNS is re-resolved when a subchannel gets GOAWAY or loses its pod, at most once every 30 s.
* **`proxy`.** Each call goes round robin to a frontend the proxy considers healthy, with fixed
  extra latency. A pod added by scaling joins after `proxy_discovery`.

Not modelled:

* the cost of TLS handshakes on reconnects;
* DNS caching;
* cross-AZ latency and data-transfer charges;
* ALB least-outstanding-requests balancing;
* sidecar CPU.
