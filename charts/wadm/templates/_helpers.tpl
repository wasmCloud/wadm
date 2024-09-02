{{/*
Expand the name of the chart.
*/}}
{{- define "wadm.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "wadm.fullname" -}}
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
{{- define "wadm.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "wadm.labels" -}}
helm.sh/chart: {{ include "wadm.chart" . }}
{{ include "wadm.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "wadm.selectorLabels" -}}
app.kubernetes.io/name: {{ include "wadm.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "wadm.nats.auth" -}}
{{- if  .Values.config.wadm.nats.creds.secretName -}}
- name: WADM_NATS_CREDS_FILE
  value: {{ include "wadm.nats.creds_file_path" . | quote }}
{{- else if and .Values.config.wadm.nats.creds.jwt .Values.config.wadm.nats.creds.seed -}}
- name: WADM_NATS_NKEY
  value: {{ .Values.config.wadm.nats.creds.seed | quote }}
- name: WADM_NATS_JWT
  value: {{ .Values.config.wadm.nats.creds.jwt | quote }}
{{- end }}
{{- end }}

{{- define "wadm.nats.creds_file_path" }}
{{- if .Values.config.wadm.nats.creds.secretName -}}
/etc/nats-creds/nats.creds
{{- end }}
{{- end }}

{{- define "wadm.nats.creds_volume_mount" -}}
{{- if .Values.config.wadm.nats.creds.secretName -}}
volumeMounts:
- name: nats-creds-secret-volume
  mountPath: "/etc/nats-creds"
  readOnly: true
{{- end }}
{{- end }}

{{- define "wadm.nats.creds_volume" -}}
{{- with .Values.config.wadm.nats.creds -}}
{{- if .secretName -}}
volumes:
- name: nats-creds-secret-volume
  secret:
    secretName: {{ .secretName }}
    items:
    - key: {{ .key }}
      path: "nats.creds"
{{- end }}
{{- end }}
{{- end }}
