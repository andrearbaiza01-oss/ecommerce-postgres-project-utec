# Guion de sustentación

**Tarea 2 — Construcción de Base de Datos**
Plataforma de datos end-to-end: Excel → PostgreSQL → Databricks (medallón) → modelo de churn

Duración objetivo: **15–18 minutos** de exposición + preguntas.

---

## 0. Lo que tienes que poder decir en 30 segundos

> "Tomé un Excel de 30 registros de ventas, lo modelé como un data warehouse dimensional
> en PostgreSQL, construí sobre él una arquitectura medallón en Databricks —bronce, plata,
> oro— y en la capa oro entrené un modelo de predicción de abandono de clientes que
> alcanza 0,846 de AUC en validación fuera de tiempo. El dato original nunca se altera y
> está validado contra los propios reportes del Excel: 34 de 34 controles de calidad en
> verde."

Si solo recuerdas una cosa de todo este documento, que sea ese párrafo.

---

## 1. Antes de empezar: ten esto abierto

| Pestaña | Para qué |
|---|---|
| pgAdmin (`localhost:5051`) o una terminal con `psql` | mostrar que el dato vive en Postgres |
| Databricks → `06_calidad_y_monitoreo` **ya ejecutado** | es tu prueba de integridad |
| Databricks → Catalog → `tarea2_gold` | mostrar las tablas finales |
| `docs/ARQUITECTURA.md` | por si piden justificar una decisión |

Ejecuta el notebook 06 **antes** de entrar a la sala. No lo corras en vivo: si el
cluster serverless tarda en levantar, pierdes tres minutos de tu exposición mirando un
spinner.

---

## 2. Guion por bloques

### Bloque 1 — El diagnóstico (2 min) · *Abre el Excel*

Empieza por aquí. Es tu bloque más fuerte y casi nadie lo hace.

> "Lo primero que hice no fue cargar el archivo, fue leerlo. El enunciado lo describe como
> un e-commerce, pero el Excel tiene **30 filas y ninguna columna de fecha**. Cada cliente
> aparece exactamente una vez. Eso no es un event stream: es una foto sin tiempo.
>
> Y eso importa porque **el churn es un fenómeno temporal**. Si un cliente compró una sola
> vez y no sé cuándo, no puedo saber si se fue. Sobre este archivo tal como está, un modelo
> de churn es literalmente imposible.
>
> Así que tomé una decisión y la documenté: separar lo real de lo derivado."

**Por qué abrir así:** cualquiera puede cargar un Excel. Demostrar que entendiste que el
dato no daba para lo que te pedían, y resolverlo sin fingir, es criterio de ingeniero.
Si no lo dices tú, te lo van a preguntar — y entonces suena a excusa en vez de a diseño.

---

### Bloque 2 — Qué es real y qué es derivado (2 min)

Este es el bloque de honestidad. Dilo sin rodeos y con el cuadro a la vista:

| Es **real**, del Excel | Es **derivado**, generado |
|---|---|
| Los 30 registros de venta | La línea de tiempo (18 meses) |
| 19 productos, precios, categorías | 1 500 clientes sintéticos |
| Sucursales, vendedores, métodos de pago | 125 319 eventos de navegación y compra |
| Las fórmulas (IGV 18 %, margen 25 %, TC 3,75) | El comportamiento de abandono |
| Los reportes declarados en la hoja | |

Tres frases que cierran el tema:

1. **El dato real no se tocó.** Está íntegro en `raw` y `core`, y se valida contra los
   totales que el propio Excel declara.
2. **Lo generado usa solo insumos reales**: los precios, el catálogo y las reglas de
   negocio salen del Excel. Lo que se simula es *cuándo* ocurren las cosas.
3. **Es reproducible.** Semilla fija `STREAM_SEED=20261117`. Cualquiera corre el script y
   obtiene exactamente los mismos 125 319 eventos.

> "Y algo importante: **no planté el churn**. No marqué clientes como 'se van'. Generé
> comportamiento y la etiqueta sale después, aplicando la misma regla de inactividad que
> aplicaría en producción. Si hubiera plantado la etiqueta, el modelo tendría 0,99 de AUC
> y no serviría para nada."

---

### Bloque 3 — PostgreSQL: el modelo dimensional (3 min) · *Abre pgAdmin*

Muestra el esquema y explica la separación:

```
raw     → el Excel tal cual, sin transformar, con hash por fila
core    → modelo estrella: 5 dimensiones + dim_fecha + fact_venta
stream  → event stream particionado por mes
mart    → vistas de reporte, equivalentes a las fórmulas del Excel
audit   → bitácora de cargas, resultados de calidad, watermarks
```

Tres cosas que vale la pena señalar porque demuestran oficio:

**Columnas generadas.** Las fórmulas del Excel no se replican en código de aplicación,
viven en la tabla:

```sql
igv_pen GENERATED ALWAYS AS (cantidad * precio_unitario_pen * 0.18) STORED
```

> "Si el cálculo vive en el Python que carga, mañana alguien inserta por otro lado y el
> IGV queda mal. En la tabla, es imposible que un registro exista sin su IGV correcto."

**Particionamiento.** `stream.evento` está particionada por rango mensual.

> "Con 125 mil eventos no hace falta, lo sé. Está hecho así porque el día que sean 125
> millones, la consulta de un mes toca una partición y no la tabla completa. El diseño se
> decide pensando en el volumen al que vas, no en el que tienes."

**Ingesta incremental por watermark.** Marca sobre `ingestion_ts`, no sobre `event_ts`.

> "Si marcara por fecha del evento, un evento que llega tarde —una app que estuvo sin
> señal tres días— quedaría por debajo de la marca y no se cargaría nunca. Marcando por
> fecha de ingesta, se recupera."

---

### Bloque 4 — La arquitectura medallón (3 min) · *Abre el Catalog de Databricks*

Recorre las tres capas diciendo **qué problema resuelve cada una**, no qué contiene:

**Bronce — fidelidad.**
> "Copia fiel del origen, sin limpiar nada, más columnas de linaje: de qué archivo vino,
> cuándo se ingirió, con qué lote. Si mañana aparece un número raro en un reporte, puedo
> rastrearlo hasta el archivo exacto que lo trajo. Bronce no existe para consultarse,
> existe para poder reconstruir."

**Plata — confianza.**
> "Aquí el dato se vuelve usable: tipado, deduplicado, texto normalizado, dimensiones
> conformadas con MERGE. Y lo que no pasa las reglas **no se descarta: se pone en
> cuarentena** en una tabla de rechazos. Un registro que desaparece sin dejar rastro es un
> bug que nadie va a encontrar."

**Oro — consumo.**
> "Tablas listas para el negocio: los reportes del Excel reproducidos, el cliente 360,
> cohortes, embudo de conversión, y las tablas del modelo de churn."

Si te queda tiempo, muestra el embudo — es visualmente convincente:

```
21 909 sesiones → 58 616 vistas → 17 223 carritos → 8 834 checkouts → 6 705 órdenes
```

---

### Bloque 5 — El modelo de churn (4 min) · *Este es el bloque que decide tu nota*

**Primero la definición, siempre.** Un modelo sin definición clara de la variable objetivo
no es un modelo.

> "Este es un negocio **no contractual**: nadie se da de baja, simplemente deja de comprar.
> Así que el churn hay que definirlo. Mi definición: un cliente activo que **no coloca
> ninguna orden en los 90 días siguientes al corte**. Los 90 días son aproximadamente tres
> veces el intervalo típico entre compras de esta base; no es un número arbitrario."

**Luego la validación out-of-time.** Aquí es donde se separan los trabajos buenos:

> "No usé un split aleatorio, y quiero explicar por qué. Construí **6 cortes mensuales**:
> entreno con los 4 más antiguos y evalúo con los 2 más recientes.
>
> Si hubiera partido al azar, el mismo cliente habría caído en entrenamiento y en prueba
> al mismo tiempo —aparece en varios cortes— y las métricas habrían salido infladas. El
> corte por fecha responde la pregunta real: **¿un modelo entrenado con el pasado funciona
> sobre el futuro?** Que es exactamente lo que le voy a pedir en producción."

**Las métricas:**

| | AUC ROC | PR-AUC | KS | Recall | Precisión |
|---|---|---|---|---|---|
| Regresión logística | 0,8433 | 0,7134 | **0,5678** | **0,832** | 0,622 |
| **GBT (promovido)** | **0,8460** | **0,7565** | 0,5441 | 0,749 | 0,632 |

Dos comentarios que debes hacer tú, sin que te los pidan:

> "El AUC en validación cruzada fue 0,849 y en prueba 0,846. Casi idénticos: el modelo no
> está sobreajustado.
>
> Y fíjense que **la logística tiene mejor recall**: 0,83 contra 0,75. Promoví el GBT por
> AUC, pero en una campaña de retención real el costo de no detectar a quien se va suele
> ser mayor que el de contactar a alguien que se iba a quedar. Con ese criterio, la
> logística sería la mejor elección. Ambas quedan registradas en
> `churn_modelo_metricas` para poder sustentar cualquiera de las dos."

**Anti-fuga de información** (menciónalo, aunque sea rápido):

> "Todas las variables se calculan solo con eventos anteriores al corte. La etiqueta mira
> solo después. Y hay un control automático que verifica que ninguna recencia sea
> negativa, porque una recencia negativa significaría que una variable vio el futuro."

---

### Bloque 6 — De la probabilidad a la acción (2 min)

Este bloque es el que convierte un ejercicio académico en algo que un negocio usaría.

> "Un modelo que entrega un número entre 0 y 1 no le sirve a nadie en comercial. Así que
> `churn_scores` entrega cuatro cosas más:
>
> - **Prioridad P1 a P4**, cruzando riesgo con valor: no es lo mismo perder a un cliente
>   de 50 soles al mes que a uno de 2 000.
> - **Ingreso en riesgo en soles** = probabilidad × venta mensual × 3 meses. Esto permite
>   comparar el problema contra el costo de la campaña.
> - **Motivo principal**, en lenguaje humano: *'lleva más del doble de su ciclo habitual
>   sin comprar'*.
> - **Acción sugerida**, distinta para B2B y B2C y según el tipo de fricción detectada.
>
> Son 665 clientes puntuados. No son los 1 530 de la base: los que llevan menos de 180
> días no son elegibles, porque puntuar a alguien registrado hace dos semanas es ruido,
> no predicción."

---

### Bloque 7 — Cierre: calidad y observabilidad (2 min) · *Abre el notebook 06*

Termina aquí. Es el bloque que deja la sensación de "esto está terminado".

> "Todo el pipeline está instrumentado. El notebook 06 reconcilia los conteos capa por
> capa, desde el Excel hasta el scoring, y verifica que **bronce = plata + rechazos**: no
> se perdió ni se inventó un solo registro en el camino.
>
> Los 34 controles de calidad están en verde, contrastados contra los totales que el
> propio Excel declara en su hoja de reportes. No contra lo que yo calculé: contra lo que
> el archivo original dice que debería dar.
>
> Los controles tienen severidad: un ERROR corta el pipeline, un WARN queda registrado.
> El criterio es que un dato incompleto es menos dañino que un dato incorrecto propagado
> a la capa siguiente.
>
> Y como las tablas son Delta, cada escritura tiene historial: puedo consultar cualquier
> tabla tal como estaba hace tres versiones."

---

## 3. Cifras para memorizar

| Dato | Valor |
|---|---|
| Registros reales del Excel | **30** (20 columnas) |
| Controles de calidad | **34 / 34 en verde** |
| Clientes | 1 530 (**30 reales** + 1 500 sintéticos) |
| Productos | 19 |
| Eventos generados | **125 319** en 13 tipos |
| Órdenes | 6 705 |
| Período | mayo 2025 – noviembre 2026 (18 meses) |
| Filas de entrenamiento | 3 284 en 6 cortes mensuales |
| Tasa de churn | 37 % |
| **AUC del modelo promovido** | **0,8460** (GBT) |
| KS | 0,5441 |
| Clientes puntuados | 665 |
| Ventana de churn | 90 días de inactividad |

---

## 4. Preguntas difíciles, con respuesta

**«Los datos son inventados, entonces el modelo no vale.»**
> "El modelo no mide el churn de este negocio, correcto, y así está documentado en las
> limitaciones. Lo que sí demuestra es que la metodología es correcta y que el pipeline
> funciona de punta a punta. Con datos reales cambia el número; el código no cambia una
> línea. Y la alternativa era no entregar modelo, porque 30 filas sin fechas no dan para
> uno."

**«¿Por qué no usaste un split 80/20 normal?»**
> "Porque el mismo cliente aparece en varios cortes mensuales. Un split aleatorio habría
> puesto al mismo cliente en entrenamiento y prueba, y las métricas habrían salido
> infladas. El corte temporal evita eso y además responde la pregunta de producción:
> entrenar con el pasado, predecir el futuro."

**«¿Por qué 90 días y no 30 o 180?»**
> "Es aproximadamente tres veces el intervalo medio entre compras de esta base. Con 30
> días marcaría como perdido a quien simplemente se demoró; con 180 me enteraría tan tarde
> que ya no hay nada que hacer. Está parametrizado en el notebook: `CHURN_DIAS_INACTIVIDAD`.
> Cambiarlo es una línea."

**«¿Cómo sé que no hay fuga de información?»**
> "Tres candados: las variables solo leen eventos anteriores al corte; el último corte
> entrenable es la fecha máxima menos 90 días, porque sin ventana futura completa la
> etiqueta sería falsamente 'churn'; y hay un control automático que falla si alguna
> recencia sale negativa."

**«¿Por qué Postgres y Databricks, y no solo uno?»**
> "Cumplen funciones distintas. Postgres es el sistema operacional: transaccional,
> normalizado, con integridad referencial y columnas generadas que garantizan los
> cálculos. Databricks es la plataforma analítica: escala horizontal, Delta Lake, ML.
> Meter el entrenamiento en Postgres o las restricciones de integridad en Databricks sería
> usar cada herramienta para lo que no es."

**«¿Por qué una capa bronce si igual la limpias después?»**
> "Porque si limpio al ingerir y la regla de limpieza resulta estar mal, el dato original
> ya se perdió. Bronce me permite reconstruir plata y oro cuantas veces haga falta sin
> volver a tocar el origen. Es la diferencia entre un pipeline recuperable y uno que, si
> se equivoca, hay que rehacer desde el archivo."

**«¿El modelo está en producción?»**
> "Está registrado en MLflow con su run, sus métricas y sus artefactos, y existe el job
> con los seis notebooks encadenados y el schedule configurado, en pausa. Lo que falta
> para producción real es el monitoreo de deriva corriendo en el tiempo —la tabla
> `churn_monitoreo` ya guarda la línea base para eso— y una prueba A/B contra un grupo de
> control para medir el impacto de las campañas."

**«¿Qué harías distinto con más tiempo?»**
> "Tres cosas: dimensiones tipo 2 para conservar el histórico de cambios; una prueba A/B
> real para medir si la campaña de retención funciona, porque el modelo predice pero no
> prueba que la intervención sirva; y calibración de probabilidades, porque el GBT ordena
> muy bien pero sus probabilidades no están calibradas, y el 'ingreso en riesgo' en soles
> depende de que lo estén."

---

## 5. Errores que debes evitar

- **No digas "los datos son reales".** No lo son del todo, está documentado, y si te
  pillan se cae tu credibilidad entera. Di siempre "30 registros reales, el resto
  derivado y documentado".
- **No corras notebooks en vivo.** Ten todo ejecutado y muestra resultados.
- **No te metas en el código línea por línea** salvo que te lo pidan. Explica decisiones,
  no sintaxis.
- **No prometas que el 0,846 de AUC se replica con datos reales.** Di que la metodología
  se sostiene; el número se vuelve a medir.
- **Si no sabes algo, dilo.** "No lo medí, pero lo verificaría así..." suena
  infinitamente mejor que inventar una cifra que luego no cuadra.

---

## 6. Si te dan menos tiempo del previsto

Orden de recorte, de lo primero que sacrificas a lo último:

1. El embudo de conversión y las cohortes (bonito, no esencial)
2. El detalle del particionamiento y el watermark
3. El recorrido por las tres capas → resúmelo en una frase por capa
4. La tabla comparativa de modelos → quédate solo con el AUC del ganador

**Nunca recortes:** el diagnóstico del Excel (bloque 1), la separación real/derivado
(bloque 2), la validación out-of-time (bloque 5) y los 34 controles en verde (bloque 7).
Esos cuatro son los que sostienen la nota.
