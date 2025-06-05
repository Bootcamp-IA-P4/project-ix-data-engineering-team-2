#!/bin/bash

# Crear estructura de directorios
mkdir -p \
  .github/workflows \
  config \
  data/schemas \
  data/samples \
  docs \
  scripts \
  src/kafka \
  src/database \
  src/etl \
  src/api \
  src/monitoring/grafana_dashboards \
  src/utils \
  tests/unit \
  tests/integration

# Crear archivos .gitkeep en todas las carpetas
#find . -type d -exec touch {}/.gitkeep \;
# Crear archivo .gitkeep sólo en carpetas vacías
find . -type d -empty -exec touch {}/.gitkeep \;

# Crear archivos base del proyecto (sin contenido)
touch \
  .env.template \
  docker-compose.yml \
  Dockerfile \
  .gitignore \
  README.md

# Eliminar .gitkeep de las carpetas que tendrán archivos (opcional)
#rm .github/workflows/.gitkeep \
#   config/.gitkeep \
#   data/schemas/.gitkeep \
#   data/samples/.gitkeep \
#   docs/.gitkeep \
#   scripts/.gitkeep \
#   src/kafka/.gitkeep \
#   src/database/.gitkeep \
#   src/etl/.gitkeep \
#   src/api/.gitkeep \
#   src/monitoring/grafana_dashboards/.gitkeep \
#   src/utils/.gitkeep \
#   tests/unit/.gitkeep \
#   tests/integration/.gitkeep

echo "Estructura de directorios creada exitosamente con .gitkeep"
echo "Archivos base del proyecto creados:"
echo "- .env.template"
echo "- docker-compose.yml"
echo "- Dockerfile"
echo "- .gitignore"
echo "- README.md"
























