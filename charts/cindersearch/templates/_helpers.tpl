{{/*
Expand the name of the chart.
*/}}
{{- define "cindersearch.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "cindersearch.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "cindersearch.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "cindersearch.labels" -}}
helm.sh/chart: {{ include "cindersearch.chart" . }}
{{ include "cindersearch.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "cindersearch.selectorLabels" -}}
app.kubernetes.io/name: {{ include "cindersearch.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Component-specific selector labels
*/}}
{{- define "cindersearch.componentSelectorLabels" -}}
{{ include "cindersearch.selectorLabels" .root }}
app.kubernetes.io/component: {{ .component }}
{{- end }}

{{/*
Component-specific full labels
*/}}
{{- define "cindersearch.componentLabels" -}}
{{ include "cindersearch.labels" .root }}
app.kubernetes.io/component: {{ .component }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "cindersearch.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "cindersearch.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Container image reference
*/}}
{{- define "cindersearch.image" -}}
{{- $tag := default .Chart.AppVersion .Values.image.tag -}}
{{- printf "%s:%s" .Values.image.repository $tag -}}
{{- end }}

{{/*
MinIO fullname helper
*/}}
{{- define "cindersearch.minio.fullname" -}}
{{- printf "%s-minio" (include "cindersearch.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end }}

{{/*
Validate chart configuration combinations early during helm template/install
*/}}
{{- define "cindersearch.validateValues" -}}
{{- $dataCount := int .Values.data.replicaCount -}}
{{- $coordEnabled := .Values.coordinator.enabled -}}
{{- $coordCount := int .Values.coordinator.replicaCount -}}
{{- $readerEnabled := .Values.reader.enabled -}}
{{- $readerCount := int .Values.reader.replicaCount -}}
{{- if lt $dataCount 1 -}}
{{- fail "data.replicaCount must be >= 1 (at least one data node is required to own shards)" -}}
{{- end -}}
{{- if and (eq .Values.s3.mode "embedded") (or (gt $dataCount 1) (and $coordEnabled (gt $coordCount 0)) (and $readerEnabled (gt $readerCount 0))) -}}
{{- fail "s3.mode=embedded is in-memory per pod and only supported for single-node dev (data.replicaCount=1, coordinator.enabled=false, reader.enabled=false). Use s3.mode=minio or s3.mode=external for multi-node clusters." -}}
{{- end -}}
{{- if and (eq .Values.s3.mode "minio") (eq .Values.s3.minio.mode "distributed") (lt (int .Values.s3.minio.replicaCount) 4) -}}
{{- fail "s3.minio.replicaCount must be >= 4 when s3.minio.mode=distributed (MinIO erasure coding minimum)" -}}
{{- end -}}
{{- if and (eq .Values.s3.mode "external") (not .Values.s3.external.existingSecret) (or (empty .Values.s3.external.accessKey) (empty .Values.s3.external.secretKey)) -}}
{{- fail "When s3.mode=external, set either s3.external.existingSecret or both s3.external.accessKey and s3.external.secretKey" -}}
{{- end -}}
{{- if le (int .Values.cluster.discovery.leaseTTLSec) (int .Values.cluster.discovery.heartbeatSec) -}}
{{- fail "cluster.discovery.leaseTTLSec must be strictly greater than cluster.discovery.heartbeatSec" -}}
{{- end -}}
{{- end }}

{{/*
Resolve S3 endpoint URL passed to CinderSearch pods.
Always returns a non-empty endpoint for minio/external so CinderSearch never falls back to embedded mock S3.
*/}}
{{- define "cindersearch.s3Endpoint" -}}
{{- if eq .Values.s3.mode "minio" -}}
{{- printf "http://%s.%s.svc.%s:%d" (include "cindersearch.minio.fullname" .) .Release.Namespace .Values.cluster.domain (int .Values.s3.minio.service.port) -}}
{{- else if eq .Values.s3.mode "external" -}}
{{- if .Values.s3.external.endpoint -}}
{{- .Values.s3.external.endpoint | trimSuffix "/" -}}
{{- else -}}
{{- printf "https://s3.%s.amazonaws.com" .Values.s3.region -}}
{{- end -}}
{{- end -}}
{{- end }}

{{/*
Resolve Kubernetes Secret name holding S3 credentials
*/}}
{{- define "cindersearch.s3SecretName" -}}
{{- if eq .Values.s3.mode "minio" -}}
{{- default (printf "%s-s3-credentials" (include "cindersearch.fullname" .)) .Values.s3.minio.existingSecret -}}
{{- else if eq .Values.s3.mode "external" -}}
{{- default (printf "%s-s3-credentials" (include "cindersearch.fullname" .)) .Values.s3.external.existingSecret -}}
{{- end -}}
{{- end }}

{{/*
Resolve Secret key names for S3 access key & secret key
*/}}
{{- define "cindersearch.s3AccessKeySecretKey" -}}
{{- if eq .Values.s3.mode "minio" -}}
{{- .Values.s3.minio.rootUserKey -}}
{{- else -}}
{{- .Values.s3.external.accessKeySecretKey -}}
{{- end -}}
{{- end }}

{{- define "cindersearch.s3SecretKeySecretKey" -}}
{{- if eq .Values.s3.mode "minio" -}}
{{- .Values.s3.minio.rootPasswordKey -}}
{{- else -}}
{{- .Values.s3.external.secretKeySecretKey -}}
{{- end -}}
{{- end }}

{{/*
Build deterministic --peers list spanning all data, coordinator, and reader StatefulSet pods:
format: id=fqdn:port@role1+role2,...
*/}}
{{- define "cindersearch.peers" -}}
{{- $fullname := include "cindersearch.fullname" . -}}
{{- $ns := .Release.Namespace -}}
{{- $domain := .Values.cluster.domain -}}
{{- $port := int .Values.engine.listenPort -}}
{{- $peers := list -}}

{{- $dataRoles := join "+" .Values.data.roles -}}
{{- range $i := until (int .Values.data.replicaCount) -}}
{{- $pod := printf "%s-data-%d" $fullname $i -}}
{{- $fqdn := printf "%s.%s-data-headless.%s.svc.%s:%d" $pod $fullname $ns $domain $port -}}
{{- $peers = append $peers (printf "%s=%s@%s" $pod $fqdn $dataRoles) -}}
{{- end -}}

{{- if and .Values.coordinator.enabled (gt (int .Values.coordinator.replicaCount) 0) -}}
{{- $coordRoles := join "+" .Values.coordinator.roles -}}
{{- range $i := until (int .Values.coordinator.replicaCount) -}}
{{- $pod := printf "%s-coordinator-%d" $fullname $i -}}
{{- $fqdn := printf "%s.%s-coordinator-headless.%s.svc.%s:%d" $pod $fullname $ns $domain $port -}}
{{- $peers = append $peers (printf "%s=%s@%s" $pod $fqdn $coordRoles) -}}
{{- end -}}
{{- end -}}

{{- if and .Values.reader.enabled (gt (int .Values.reader.replicaCount) 0) -}}
{{- $readerRoles := join "+" .Values.reader.roles -}}
{{- range $i := until (int .Values.reader.replicaCount) -}}
{{- $pod := printf "%s-reader-%d" $fullname $i -}}
{{- $fqdn := printf "%s.%s-reader-headless.%s.svc.%s:%d" $pod $fullname $ns $domain $port -}}
{{- $peers = append $peers (printf "%s=%s@%s" $pod $fqdn $readerRoles) -}}
{{- end -}}
{{- end -}}

{{- join "," $peers -}}
{{- end }}

{{/*
Build pod anti-affinity block for a component pool (soft | hard | none)
Usage: include "cindersearch.poolAffinity" (dict "root" . "component" "data" "antiAffinity" .Values.data.antiAffinity "custom" .Values.data.affinity)
*/}}
{{- define "cindersearch.poolAffinity" -}}
{{- if .custom }}
{{- toYaml .custom }}
{{- else if eq .antiAffinity "hard" }}
podAntiAffinity:
  requiredDuringSchedulingIgnoredDuringExecution:
    - labelSelector:
        matchLabels:
          app.kubernetes.io/name: {{ include "cindersearch.name" .root }}
          app.kubernetes.io/instance: {{ .root.Release.Name }}
          app.kubernetes.io/component: {{ .component }}
      topologyKey: kubernetes.io/hostname
{{- else if eq .antiAffinity "soft" }}
podAntiAffinity:
  preferredDuringSchedulingIgnoredDuringExecution:
    - weight: 100
      podAffinityTerm:
        labelSelector:
          matchLabels:
            app.kubernetes.io/name: {{ include "cindersearch.name" .root }}
            app.kubernetes.io/instance: {{ .root.Release.Name }}
            app.kubernetes.io/component: {{ .component }}
        topologyKey: kubernetes.io/hostname
{{- end }}
{{- end }}
