{{/*
Expand the name of the chart.
*/}}
{{- define "opamp-server.name" -}}
{{- default .Release.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}
