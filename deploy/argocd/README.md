# Helios Argo CD Applications

These manifests let Argo CD continuously deploy Helios from the Kustomize overlays.

## Dev

```bash
kubectl apply -f deploy/argocd/helios-dev-application.yaml
```

The dev app tracks `deploy/k8s/overlays/dev`, prunes deleted resources, and self-heals drift.

## Prod-like

```bash
kubectl apply -f deploy/argocd/helios-prod-like-application.yaml
```

The prod-like app tracks `deploy/k8s/overlays/prod-like`, self-heals drift, and keeps pruning disabled as a safer default for demos.

Before applying either app, update `repoURL` if your GitHub repository name differs.
