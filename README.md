# CommitteeSampling
Setup-Free Committee Sampling with Subquadratic Communication

## Table of Contents
- [Overview](#overview)
- [Getting Started](#getting-started)
- [Development](#development)
- [Docker & Containerization](#docker--containerization)
- [CI/CD Pipeline](#cicd-pipeline)
- [Deployment](#deployment)
- [Monitoring](#monitoring)
- [Configuration](#configuration)
- [Contributing](#contributing)

## Overview

CommitteeSampling implements a setup-free committee sampling protocol with subquadratic communication complexity. The system provides efficient distributed consensus mechanisms for blockchain and distributed systems applications.

### Key Features
- **Setup-Free Protocol**: No trusted setup required
- **Subquadratic Communication**: Efficient message complexity
- **Multi-Environment Support**: Development, staging, and production configurations
- **Containerized Deployment**: Docker-based infrastructure
- **Comprehensive CI/CD**: Automated testing, building, and deployment

## Getting Started

### Prerequisites
- Go 1.23+
- Docker & Docker Compose
- Make
- Protocol Buffers compiler (`protoc`)

### Quick Start

"""bash
# Clone the repository
git clone <repository-url>
cd committee-sampling

# Install dependencies
make deps

# Generate protobuf files
make proto

# Build the application
make build

# Run the application
make run
"""

### Environment Variables

The application uses environment-based configuration:

- `ENV`: Environment type (`dev`, `stg`, `prod`) - defaults to `dev`
- `PORT`: Application port - defaults to `8080`

## Development

### Local Development Setup

"""bash
# Install dependencies and run in development mode
make dev

# Run tests
make test

# Generate protobuf files
make proto

# Clean build artifacts
make clean
"""

### Code Quality

The project includes automated linting and formatting:
- **golangci-lint**: Comprehensive Go linting
- **gofmt**: Code formatting
- **go vet**: Static analysis

## Docker & Containerization

### Building Docker Images

"""bash
# Build Docker image
make docker-build

# Run container with development config
make docker-run-dev

# Run container with production config  
make docker-run-prod

# Run container with staging config
make docker-run-stg

# Clean up Docker resources
make docker-clean
"""

### Docker Compose

For local development with additional services:

"""bash
# Start application stack
make compose-up

# Start with specific environment
make compose-up-dev
make compose-up-prod
make compose-up-stg

# Start with monitoring stack
make compose-monitoring

# View logs
make compose-logs

# Stop all services
make compose-down
"""

### Multi-Stage Docker Build

The Dockerfile uses multi-stage builds for optimization:
1. **Builder Stage**: Go 1.23 Alpine with build tools
2. **Runtime Stage**: Minimal Alpine with security features

Key features:
- **Security**: Non-root user execution
- **Efficiency**: Small final image size
- **Caching**: Optimized layer caching
- **Protobuf**: Automatic proto compilation

## CI/CD Pipeline

### GitHub Actions Workflow

The project includes a comprehensive CI/CD pipeline (`.github/workflows/ci-cd.yml`) with:

#### 🔄 **Continuous Integration**
- **Code Quality**: Linting, formatting, and static analysis
- **Testing**: Comprehensive test suite execution
- **Build Verification**: Multi-architecture builds
- **Security Scanning**: Vulnerability assessment with Trivy

#### 🚀 **Continuous Deployment**
- **Automatic Deployments**: Branch-based deployment strategy
- **Environment Promotion**: Dev → Staging → Production
- **Image Management**: Container registry with GitHub Packages

#### 📊 **Pipeline Stages**

1. **Test Stage**
   """bash
   - Go setup and dependency installation
   - Protobuf compilation
   - Code linting with golangci-lint
   - Unit and integration tests
   - Build artifact generation
   """

2. **Docker Build Stage**
   """bash
   - Multi-platform Docker builds
   - Container registry push (ghcr.io)
   - Image testing and validation
   - Build cache optimization
   """

3. **Security Stage**
   """bash
   - Trivy vulnerability scanning
   - SARIF report generation
   - Security findings upload to GitHub
   """

4. **Deployment Stages**
   """bash
   - Development: Triggered on 'develop' branch
   - Staging: Triggered on 'main' branch  
   - Production: Triggered on release tags
   """

### Triggering Deployments

#### Development Environment
"""bash
git push origin develop
# Automatically deploys to development environment
"""

#### Staging Environment  
"""bash
git push origin main
# Automatically deploys to staging environment
"""

#### Production Environment
"""bash
git tag v1.0.0
git push origin v1.0.0
# Automatically deploys to production environment
"""

### CI/CD Make Targets

"""bash
# CI/CD specific builds
make ci-build     # Multi-stage build with caching
make ci-test      # Container smoke tests
make ci-push      # Push to container registry
"""

## Deployment

### Environment Configuration

The application supports multiple deployment environments:

| Environment | Branch/Trigger | Config File | Description |
|-------------|----------------|-------------|-------------|
| Development | `develop` | `dev.json` | Development and testing |
| Staging | `main` | `stg.json` | Pre-production validation |
| Production | Release tags | `prod.json` | Live production system |

### Container Registry

Images are published to GitHub Container Registry:
- **Registry**: `ghcr.io`
- **Image**: `ghcr.io/<username>/committee-sampling`
- **Tags**: Branch names, PR numbers, semantic versions

### Deployment Commands

"""bash
# Pull and run latest development image
docker run -e ENV=dev ghcr.io/<username>/committee-sampling:develop

# Pull and run latest production image  
docker run -e ENV=prod ghcr.io/<username>/committee-sampling:v1.0.0
"""

## Monitoring

### Health Checks

The application includes Docker health checks:
"""bash
# Docker Compose health check endpoint
wget --quiet --tries=1 --spider http://localhost:8080/health
"""

### Monitoring Stack (Optional)

Enable monitoring with Docker Compose:
"""bash
make compose-monitoring
"""

Includes:
- **Prometheus**: Metrics collection (http://localhost:9090)
- **Grafana**: Metrics visualization (http://localhost:3000)

Default Grafana credentials:
- Username: `admin`
- Password: `admin`

## Configuration

### Environment-Based Config

The application loads configuration based on the `ENV` environment variable:

"""bash
ENV=dev     # Loads configs/dev.json
ENV=stg     # Loads configs/stg.json  
ENV=prod    # Loads configs/prod.json
"""

### Configuration Files

Configuration files are located in the `configs/` directory:
- `dev.json`: Development settings
- `stg.json`: Staging settings
- `prod.json`: Production settings

### Runtime Configuration

Key configuration sections:
- **Network**: P2P networking and discovery
- **Graph**: Protocol parameters
- **Runtime**: Session and committee settings
- **Synchronization**: Timing and consensus
- **Logger**: Logging configuration

#### Adversarial Simulation Options

The optional `network.adversary` block enables deterministic fault-injection during simulations. All flags default to `false`/zero, so production runs remain unaffected until explicitly enabled.

- `enabled`: master switch for the advanced behaviors. When set, the node evaluates additional knobs below.
- `seed`: 64-bit seed used to derive per-node RNGs (drop/jitter ordering and Merkle tampering remain reproducible across runs).
- `drop_probability`, `jitter_min`, `jitter_max`: tune unreliable links; outbound messages may be dropped or delayed by a deterministic amount within the configured range. Jitter is applied only when `enabled` is true.
- `clock_skew`: shifts the local wall-clock and synchronizer timers (useful for skew studies). Accepts Go duration strings (e.g. `"250ms"`, `"-1s"`).
- `ex_ante.equivocator`: when `true`, Ex-Ante timestamp messages are deterministically split so half the neighbors receive an altered value while signatures remain valid.
- `ex_post.freshness_cheater`: controls Ex-Post tampering. Supported fields:
  - `enabled`: master toggle for the behavior.
  - `mode`: `"stale"`, `"truncate"`, or `"both"`. `stale` resends an older challenge; `truncate` removes the last sibling from the Merkle path; `both` applies both attacks.
  - `stale_rounds`: (optional) how many rounds back to reuse the cached challenge when `mode` includes `stale` (default `1`).
  - `truncate_leaf`: optional manual override; set to `false` to force `mode="both"` to only stale challenges.

Example excerpt:

```
"network": {
  "adversary": {
    "enabled": true,
    "seed": 12345,
    "drop_probability": 0.05,
    "jitter_min": "20ms",
    "jitter_max": "150ms",
    "clock_skew": "250ms",
    "ex_ante": { "equivocator": true },
    "ex_post": {
      "freshness_cheater": {
        "enabled": true,
        "mode": "both",
        "stale_rounds": 2
      }
    }
  }
}
```

These settings apply locally per node; in multi-node simulations configure each participant identically (or with distinct seeds) to reproduce desired adversarial patterns.

## Contributing

### Development Workflow

1. **Fork and Clone**
   """bash
   git clone <your-fork>
   cd committee-sampling
   """

2. **Create Feature Branch**
   """bash
   git checkout -b feature/your-feature-name
   """

3. **Develop and Test**
   """bash
   make dev      # Development mode
   make test     # Run tests
   make proto    # Generate protobuf files
   """

4. **Quality Checks**
   """bash
   # The CI pipeline will automatically run:
   # - golangci-lint
   # - go vet
   # - Unit tests
   # - Docker builds
   # - Security scans
   """

5. **Submit Pull Request**
   - Target the `develop` branch for features
   - Target the `main` branch for hotfixes
   - Include comprehensive commit messages
   - Ensure all CI checks pass

### Code Guidelines

- **Functions**: Short and focused (< 100 lines)
- **Files**: Modular and cohesive (< 200 lines)
- **Comments**: Minimal - prefer self-documenting code
- **Names**: Explicit and contextual
- **Returns**: Early return pattern preferred
- **Testing**: Comprehensive unit and integration tests

---

## Quick Reference

### Essential Commands
"""bash
# Development
make dev              # Run in development mode
make test             # Run tests
make build            # Build binary

# Docker
make docker-run-dev   # Run container (dev)
make docker-clean     # Clean Docker resources

# Docker Compose  
make compose-up-dev   # Start dev stack
make compose-down     # Stop all services
make compose-logs     # View logs

# CI/CD
make ci-build         # CI-optimized build
make ci-test          # Container tests
"""

### Environment URLs
- **Development**: http://localhost:8080
- **Monitoring**: http://localhost:3000 (Grafana), http://localhost:9090 (Prometheus)

For more detailed information, see the individual configuration files and workflow definitions.
