# Trabajo Final — Base de Datos (UTEC)

Análisis de datos de un e-commerce en PostgreSQL. Los datos son eventos de
navegación y compra: vistas de producto, agregados al carrito y compras.

## Estructura de la Repo

```
Trabajo_Final.sql                     dump completo: esquema + datos
trabajo_final_parte2.sql              las 10 consultas de negocio
trabajo_final_parte2_resultados.txt   la salida de esas consultas
trabajo_final_parte3.sql              las 6 features para Machine Learning
trabajo_final_parte3_resultados.txt   la salida de esas consultas
```

Los datos vienen del dataset [eCommerce behavior data from multi category store](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store) (REES46) de Kaggle.

## Las consultas

Las cinco primeras son básicas e intermedias, las cinco últimas son avanzadas.

| | Pregunta |
|---|---|
| Q1 | ¿Qué productos se ven más y de qué marca son? |
| Q2 | ¿Qué categorías facturan y con qué ticket promedio? |
| Q3 | ¿Qué marcas convierten mejor sus vistas en compras? |
| Q4 | ¿Cuántos usuarios registrados nunca hicieron nada? |
| Q5 | ¿Qué cobertura tiene el registro de dispositivos? |
| Q6 | ¿Cómo se reparten las sesiones en el embudo de conversión? |
| Q7 | ¿Cuáles son los 3 productos top de cada categoría? |
| Q8 | ¿Cómo evoluciona la actividad minuto a minuto? |
| Q9 | ¿Qué marcas venden por encima del ticket promedio? |
| Q10 | ¿Qué camino recorre el usuario antes de comprar? |

## Features para Machine Learning

Seis variables por usuario para un modelo de propensión de compra: predecir
si un usuario va a comprar a partir de cómo navega. Como los datos cubren una
ventana de 32 minutos, las features miden la actividad dentro de esa ventana
y no periodos como "últimos 30 días".

| Feature | Definición | Posible uso en IA |
|---|---|---|
| `vistas_usuario` | Productos vistos por el usuario | Propensión de compra, segmentación por actividad |
| `productos_distintos_vistos` | Productos diferentes que vio | Distinguir exploración de decisión; recomendación |
| `precio_promedio_visto` | Precio promedio de lo que vio | Segmento de precio, ofertas personalizadas |
| `ratio_carrito_vistas` | Agregados al carrito sobre vistas | Intención de compra, alertas de carrito abandonado |
| `minutos_activo` | Minutos entre su primer y último evento | Propensión de compra, rebote, detección de bots |
| `concentracion_marca` | Proporción de vistas de su marca más vista | Recomendación por marca, segmentación por lealtad |

La definición, la justificación y la consulta de cada una están en
`trabajo_final_parte3.sql`. Al final del archivo, D1 une las seis features
con la variable objetivo `compro` en una sola tabla, y D2 compara compradores
con no compradores:

| Grupo | Usuarios | Vistas | Ratio carrito | Minutos activo | Concentración de marca |
|---|---|---|---|---|---|
| Compradores | 39 | 5.74 | 0.174 | 7.86 | 0.84 |
| No compradores | 973 | 3.86 | 0.009 | 3.30 | 0.84 |

El ratio de carrito, los minutos activos y las vistas separan bien a los dos
grupos; la concentración de marca no. Con 39 compradores (3.9 %), un modelo
tendría que compensar el desbalance de clases y calcular las features solo con
los eventos anteriores a la compra.
