# Decisiones de diseño

El sistema resuelve un top N de frutas por cantidad total sobre un pipeline
`Gateway → Sum → Aggregation → Join → Gateway`.

A partir de un sistema base ya implementado se nos pide extender las funcionalidades 
del mismo para permitir el escalado horizontal en las distintas etapas del pipeline 
mencionado resolviendo los desafíos de sincronización y comunicación que eso implica.

## Separación de flujos por cliente (Escenario 2)

Cada `MessageHandler` del gateway genera un `client_id` con `uuid4()` y lo inyecta
en todos los mensajes (DATA, EOF). Todas las etapas mantienen su estado indexado
por `client_id` (diccionarios `*_by_client_id`), de modo que varias consultas
conviven sin interferir. Se optó por `uuid4()` para identificar clientes porque
garantiza unicidad sin coordinación entre handlers, que pueden vivir en procesos
distintos.

## Coordinación de EOF entre múltiples Sums (Escenario 3)

Esta fue la decisión central del trabajo. Con N instancias de Sum repartiéndose la
`input_queue` por round-robin, el EOF del cliente es **un único mensaje**: lo lee
una sola instancia y las demás no se enteran directamente de que el envío de datos del
cliente en cuestión terminó. Necesitamos que las N instancias de sum vuelquen su parcial
antes de poder avanzar.

El problema tiene dos caras. Una es la **exclusión** sobre el estado compartido; la
otra, es el **ordenamiento**: la señal "ya no hay más datos de este
cliente" es una *ausencia ambigua* (terminó? o falta un dato en tránsito?). Si el
EOF se procesa antes que los últimos datos, el parcial se emite incompleto.

Se evaluaron dos caminos:

- **Dos hilos (datos + control) con lock.** El lock resuelve la exclusión, pero no
  el ordenamiento: el EOF llega por un canal lateral que no respeta el FIFO de los
  datos, así que puede adelantarse a datos aún en tránsito. La propia naturaleza del
  problema impone que si separamos el tránsito no hay forma de anticipar si todavía 
  faltan datos por procesar de un cliente al momento de leer el EOF del canal donde
  las instancias de Sum se comunican. Por eso se descartó.

- **Un solo canal: work queue + control exchange serializados (camino elegido).** Se
  consume la `input_queue` y un *fanout* de control sobre **una sola conexión y un
  solo hilo**. RabbitMQ serializa los callbacks (un executor por canal), así que no 
  hay sección crítica ni hace falta lock. Cuando un Sum lee
  el EOF de la work queue, re-difunde ese EOF al exchange de control, entonces ese EOF de
  control *nace después* del EOF de datos, y como el FIFO por cola garantiza que
  para entonces todos los datos ya fueron entregados, cada Sum vuelca un parcial
  completo.

Para soportar este patrón se agregó `MessageMiddlewareMultiRabbitMQ`, que consume
una work queue y un exchange fanout sobre la misma conexión con un callback por
fuente.

## Partición por fruta y hashing determinístico (Escenario 4)

Cada Sum, al terminar, ya colapsó el volumen de su cliente en un diccionario
`fruta → cantidad`. En lugar de hacer *broadcast* de esas frutas a todos los
Aggregators (procesamiento redundante), cada fruta se envía a **un único**
Aggregator: `idx = crc32(fruta) % AGGREGATION_AMOUNT`. Así el espacio de frutas
queda particionado en subconjuntos disjuntos y cada Aggregator tiene el total
*completo* de las frutas que le tocan.

Nótese que un punto clave para que esta lógica funcione es que el hashing de los
elementos debe ser **determinístico entre procesos por fruta**. De dividirse los 
flujos de una misma fruta podría ocurrir que en un top observemos dos elementos
del estilo `manzana: 5` y `manzana: 3` en lugar de ver `manzana: 8` en el top final.
Para esto se eligió `zlib.crc32` por ser simple, determinístico y no criptográfico.

## Barreras por conteo

El fin del consumo de datos por un cliente se obtiene contando la cantidad de _resultados
definitivos_ en la fase de adelante de la pipeline leyendo cuantos deben haber
de env vars (dato conocido):

- Cada **Aggregator** espera `SUM_AMOUNT` EOFs de un cliente antes de calcular su
  top parcial. Como la partición es por fruta, un Aggregator puede recibir todos los
  EOFs de un cliente del que no tiene ninguna fruta; en ese caso emite igualmente un
  parcial **vacío**, para no colgar la barrera del Join.
- El **Join** espera `AGGREGATION_AMOUNT` parciales por cliente, hace el merge y
  emite el top final. Un parcial vacío cuenta para la barrera aunque no aporte
  frutas.

El truncado a `TOP_SIZE` en el Aggregator es una optimización: como las frutas están
particionadas, una fruta que no entra al top local de su Aggregator tampoco puede
entrar al global, así que descartarla no pierde información. El Join solo mergea
tops disjuntos y reordena, por lo que el tráfico hacia él escala con `TOP_SIZE`,
no con la cantidad de frutas. Por lo pronto al momento de ordenar no se utiliza
la información adicional de que cada top parcial está ordenado por lo que utilizando
otra técnica algorítmica para el merge podría optimizarse un poco más.

## Graceful shutdown y liberación de recursos

Sum, Aggregation y Join manejan `SIGTERM`: el handler llama a `stop_consuming` para
que el loop de consumo retorne, y el cierre de todas las conexiones se hace en un
`finally`, garantizando que se liberen aunque el consumo termine por excepción.

A diferencia del tp anterior, aquí `close()` se hizo **idempotente** (retorna si la
conexión ya está cerrada). El shutdown puede intentar cerrar varias conexiones en
secuencia y no queremos que el fallo de un cierre impida los siguientes; ante un
error genuino de cierre se sigue lanzando `MessageMiddlewareCloseError`.

## Escalabilidad

- **Clientes:** el estado por `client_id` en cada etapa permite atender múltiples
  consultas concurrentes sin interferencia. Sumar clientes no requiere cambios.
- **Grandes volúmenes de datos:** el volumen se colapsa lo antes posible. El Sum
  agrega en memoria y envía un mensaje por fruta distinta, no por dato; el Aggregator
  trunca a `TOP_SIZE`. El canal de control transporta solo señales (EOFs), nunca
  datos, así que su costo no crece con el volumen. Escalar el volumen se acompaña
  agregando instancias de Sum, que se reparten la ingesta por round-robin con
  `prefetch=1` (reparto justo).
- **Cantidad de controles:** subir `SUM_AMOUNT` o `AGGREGATION_AMOUNT` es una
  cuestión de configuración; las barreras se ajustan solas por env var y la partición
  por fruta redistribuye el trabajo entre los Aggregators disponibles.
