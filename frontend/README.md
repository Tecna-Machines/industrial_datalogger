# Dashboard de Producción

Frontend para consumir la API de `api_mysql_reader.py` y mostrar los datos de producción en tiempo real.

## Características

- **Dashboard moderno** con diseño responsivo usando React y Tailwind CSS
- **Auto-refresco** configurable con intervalo personalizable (en segundos)
- **Últimos 10 registros** obtenidos del endpoint `/latest?n=10`
- **Resumen estadístico** con tarjetas de métricas clave
- **Estado visual** con colores para indicar estado activo/detenido
- **Control de pausa/reanudación** para el auto-refresco
- **Actualización manual** con botón de refresh

## Instalación

1. Instalar dependencias:
```bash
cd frontend
npm install
```

2. Asegurarse que la API está corriendo en `http://localhost:8080`:
```bash
uvicorn api_mysql_reader:app --host 0.0.0.0 --port 8080
```

3. Iniciar el frontend:
```bash
npm start
```

La aplicación se abrirá en `http://localhost:3000`

## Uso

1. **Configurar intervalo**: Use el campo "Intervalo de refresco" para definir cada cuántos segundos se actualizarán los datos (1-300 segundos)
2. **Pausar/Reanudar**: Use el botón para pausar o reanudar el auto-refresco
3. **Actualización manual**: Use el botón "Actualizar ahora" para refrescar los datos inmediatamente
4. **Visualización**: Los datos se muestran en una tabla con los últimos 10 registros, destacando el más reciente

## Datos mostrados

- ID Función
- Fecha y hora
- Producto
- Estado (Activo/Detenido)
- Turno
- Piezas Totales/Good/Bad
- OEE (%)
- Tiempo de producción

## Métricas resumen

- Total de piezas producidas
- Piezas buenas y malas
- OEE promedio

## Tecnologías

- React 18
- Tailwind CSS
- Lucide React (iconos)
- Fetch API (comunicación con backend)
