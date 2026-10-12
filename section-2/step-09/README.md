# Miles and Smiles - Kubernetes Deployment

This step deploys the multi-agent car management system to the existing
`ardavan-ghaffari-dev` Red Hat Developer Sandbox project. It uses an OpenShift
Route, which automatically generates the hostname and terminates TLS.

## Architecture

The deployment consists of:

- **Main Application** (`miles-and-smiles`): Multi-agent system with supervisor pattern
- **A2A Agent** (`miles-and-smiles-a2a`): Remote pricing agent accessed via Agent-to-Agent protocol
- **PostgreSQL**: Database for car inventory and management data

## Prerequisites

- OpenShift CLI credentials for the `ardavan-ghaffari-dev` project
- `kubectl` CLI configured to use the same cluster credentials
- JBang installed (`curl -Ls https://sh.jbang.dev | bash -s - app setup`)
- OpenAI API key
- Access to the Sandbox integrated image registry

## Quick Start

### One-Command Deployment

1. **Set your OpenAI API key:**
   ```bash
   export OPENAI_API_KEY=your-key-here
   ```

2. **Log in to the integrated registry.** This is required before the image
   push. It stores a registry token in your local container-tool credentials;
   it does not create or change a cluster resource.

   ```bash
   oc registry login
   ```

   If the Quarkus image push does not find those credentials, authenticate the
   container tool used by your Quarkus container-image extension with the token
   from `oc whoami -t`, then rerun the push.

3. **Deploy everything with a single command:**
   ```bash
   ./deploy.java all
   ```

This will automatically:
- Build both container images
- Push them to the registry
- Create secrets (including your OPENAI_API_KEY)
- Deploy PostgreSQL
- Deploy the A2A agent
- Deploy the main application

4. **Get the application URL:**
   ```bash
   ./deploy.java get-route
   ```

### Step-by-Step Deployment

If you prefer to run each step individually:

1. **Set your OpenAI API key:**
   ```bash
   export OPENAI_API_KEY=your-key-here
   ```

2. **Log in to the integrated registry:**
   ```bash
   oc registry login
   ```

3. **Build container images:**
   ```bash
   ./deploy.java build
   ```

4. **Push images to registry:**
   ```bash
   ./deploy.java push
   ```

5. **Deploy to Kubernetes:**
   ```bash
   ./deploy.java deploy
   ```

6. **Get the application URL:**
   ```bash
   ./deploy.java get-route
   ```

## Available Commands

The `deploy.java` JBang script provides the following commands:

- `./deploy.java all` - **Build, push, and deploy everything in one command**
- `./deploy.java build` - Build both container images locally
- `./deploy.java push` - Build and push images to registry
- `./deploy.java deploy` - Deploy all components to Kubernetes (creates secrets)
- `./deploy.java undeploy` - Remove all components
- `./deploy.java get-route` - Get the external application URL
- `./deploy.java logs-main` - View main application logs
- `./deploy.java logs-a2a` - View A2A agent logs
- `./deploy.java logs-postgres` - View PostgreSQL logs
- `./deploy.java port-forward` - Forward app to localhost:8080 (fallback)
- `./deploy.java clean` - Clean build artifacts

## Configuration

### Container Registry

The image push targets this Sandbox registry and project:

```properties
quarkus.container-image.registry=default-route-openshift-image-registry.apps.rm3.7wse.p1.openshiftapps.com
quarkus.container-image.group=ardavan-ghaffari-dev
```

The OpenShift Route (`kubernetes/route.yaml`) automatically generates a hostname and provides TLS termination. This is deployed by default with `./deploy.java all` or `./deploy.java deploy`.

To get the URL:
```bash
./deploy.java get-route
```

### OpenAI Configuration

The OpenAI base URL and model can be configured in `kubernetes/configmap.yaml`:

```yaml
data:
  OPENAI_BASE_URL: "https://api.openai.com/v1"
  OPENAI_MODEL_NAME: "gpt-4o"
```

### A2A Agent Configuration

The main application connects to the remote A2A pricing agent using the Agent-to-Agent protocol. The A2A client discovers the agent's endpoint by fetching the agent card from `/.well-known/agent-card.json`.

**Development Mode:**
- Main app: `http://localhost:8080`
- A2A agent: `http://localhost:8888` (to avoid port conflicts)

**Production Mode (Kubernetes):**
- Main app: `http://miles-and-smiles.ardavan-ghaffari-dev.svc.cluster.local`
- A2A agent: `http://miles-and-smiles-a2a.ardavan-ghaffari-dev.svc.cluster.local` (port 80 → 8080)

#### Agent Card Configuration

The A2A agent card must return the correct URL for the environment. This is configured in the A2A agent's `application.properties`:

**A2A Agent** (`remote-a2a-agent/src/main/resources/application.properties`):
```properties
# Base URL for agent card - used by PricingAgentCard
a2a.base-url=http://localhost:8888/
%prod.a2a.base-url=http://miles-and-smiles-a2a.ardavan-ghaffari-dev.svc.cluster.local/
```

The `PricingAgentCard` class injects this property to generate the correct URLs in the agent card:

```java
@ConfigProperty(name = "a2a.base-url", defaultValue = "http://localhost:8888/")
String baseUrl;
```

This ensures the agent card returns the Kubernetes service URL in production, allowing the main application to connect correctly.

#### Client Configuration

The main application's `PricingAgent` interface specifies where to fetch the agent card:

```java
@A2AClientAgent(
    a2aServerUrl = "http://miles-and-smiles-a2a.miles-and-smiles.svc.cluster.local",
    ...
)
```

The A2A client fetches the agent card from this URL, extracts the actual endpoint URL from the card, and uses that for all subsequent communication.

**Important:** The A2A agent must be running and accessible before the main application starts, as it fetches the agent card during initialization.

## OpenShift Resources

The deployment creates the following resources in the existing
`ardavan-ghaffari-dev` project:

- **ConfigMap**: Application configuration (database, A2A URL, OpenAI settings)
- **Secret**: Sensitive data (OpenAI API key, database credentials)
- **PostgreSQL Deployment**: Database with ephemeral storage
- **PostgreSQL Service**: Internal database access
- **Main App Deployment**: Multi-agent system
- **Main App Service**: Internal service access
- **A2A Agent Deployment**: Remote pricing agent
- **A2A Agent Service**: Internal A2A access
- **Route**: External HTTPS access

## Database

PostgreSQL is deployed with:
- Image: `docker.io/library/postgres:18`
- Ephemeral storage (data lost on pod restart)
- Credentials stored in Kubernetes Secret
- Health probes for liveness and readiness

For production, consider using a StatefulSet with persistent volumes.

## Health Checks

Both applications include Quarkus SmallRye Health endpoints:

- `/q/health/live` - Liveness probe
- `/q/health/ready` - Readiness probe
- `/q/health/started` - Startup probe

## Troubleshooting

### Check pod status:
```bash
kubectl get pods -n ardavan-ghaffari-dev
```

### View logs:
```bash
./deploy.java logs-main
./deploy.java logs-a2a
./deploy.java logs-postgres
```

### Describe resources:
```bash
kubectl describe deployment miles-and-smiles -n ardavan-ghaffari-dev
kubectl describe route miles-and-smiles -n ardavan-ghaffari-dev
```

### Access via port-forward (if route is not working):
```bash
./deploy.java port-forward
# Then open http://localhost:8080
```

## Development vs Production

The application uses Quarkus profiles:

- **Dev mode** (`%dev`): Uses localhost PostgreSQL, port 8888 for A2A agent
- **Production** (`%prod`): Uses Kubernetes services, environment variables from secrets/configmaps

## Next Steps

- Configure persistent storage for PostgreSQL
- Set up monitoring and observability
- Configure resource limits and requests
- Set up horizontal pod autoscaling
