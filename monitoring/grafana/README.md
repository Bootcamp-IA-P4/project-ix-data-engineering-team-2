# Grafana

Esta carpeta se utiliza como volumen persistente para los datos y configuraciones de Grafana.

## Dashboards

Puedes importar dashboards de ejemplo desde archivos JSON ubicados en `monitoring/grafana/dashboards/`.

## Uso

- Accede a Grafana en http://localhost:3000 (usuario y contraseña por defecto: admin / admin).
- Añade Prometheus como fuente de datos (`http://prometheus:9090`).
- Importa dashboards desde la sección de Dashboards > Import. 