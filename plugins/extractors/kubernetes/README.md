# kubernetes

Extract infrastructure topology metadata from a Kubernetes cluster.

## Usage

```yaml
source:
  name: kubernetes
  scope: my-cluster
  config:
    kubeconfig: ~/.kube/config
    namespaces:
      - default
      - production
    extract:
      - namespaces
      - deployments
      - services
      - jobs
    exclude:
      - kube-system
      - kube-public
```

## Config

| Key | Type | Required | Default | Description |
| :-- | :--- | :------- | :------ | :---------- |
| `kubeconfig` | `string` | No | Auto-detected | Path to kubeconfig file. Falls back to `KUBECONFIG` env, in-cluster config, then `~/.kube/config`. |
| `namespaces` | `[]string` | No | All namespaces | Namespaces to extract from. |
| `extract` | `[]string` | No | `["namespaces", "deployments", "services", "jobs"]` | Resource types to extract. Pods excluded by default. |
| `exclude` | `[]string` | No | | Namespaces to exclude from extraction. |

## Entities

### `namespace`

Kubernetes namespace.

| Property | Description |
| :------- | :---------- |
| `labels` | Namespace labels |
| `status` | Namespace phase (Active, Terminating) |
| `created_at` | Creation timestamp |

### `deployment`

Kubernetes deployment.

| Property | Description |
| :------- | :---------- |
| `namespace` | Namespace name |
| `replicas` | Desired replica count |
| `ready_replicas` | Ready replica count |
| `strategy` | Deployment strategy (RollingUpdate, Recreate) |
| `containers` | List of container name/image pairs |
| `labels` | Deployment labels |
| `created_at` | Creation timestamp |

### `service`

Kubernetes service.

| Property | Description |
| :------- | :---------- |
| `namespace` | Namespace name |
| `type` | Service type (ClusterIP, NodePort, LoadBalancer) |
| `cluster_ip` | Cluster IP address |
| `ports` | List of port configurations |
| `selector` | Label selector |
| `labels` | Service labels |
| `created_at` | Creation timestamp |

### `job`

Kubernetes job.

| Property | Description |
| :------- | :---------- |
| `namespace` | Namespace name |
| `completions` | Desired completions |
| `active` | Active pod count |
| `succeeded` | Succeeded pod count |
| `failed` | Failed pod count |
| `labels` | Job labels |
| `created_at` | Creation timestamp |

## Edges

| From | To | Type | Description |
| :--- | :- | :--- | :---------- |
| `deployment` | `namespace` | `belongs_to` | Deployment belongs to a namespace |
| `service` | `namespace` | `belongs_to` | Service belongs to a namespace |
| `job` | `namespace` | `belongs_to` | Job belongs to a namespace |

## Authentication

The extractor supports three authentication methods, tried in order:

1. **Explicit kubeconfig** — set `kubeconfig` in config
2. **In-cluster** — uses service account token at `/var/run/secrets/kubernetes.io/serviceaccount/token`
3. **Default kubeconfig** — `~/.kube/config`

Both bearer token and client certificate authentication from kubeconfig are supported.

## Contribute

Refer to the [contribution guidelines](../../../docs/contribute/guide.mdx#adding-a-new-extractor) for information on contributing to this module.
