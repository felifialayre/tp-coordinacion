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

Se evaluaron tres caminos:

- **Dos hilos (datos + control) con lock.** El lock resuelve la exclusión, pero no
  el ordenamiento: el EOF llega por un canal lateral que no respeta el FIFO de los
  datos, así que puede adelantarse a datos aún en tránsito. La propia naturaleza del
  problema impone que si separamos el tránsito no hay forma de anticipar si todavía 
  faltan datos por procesar de un cliente al momento de leer el EOF del canal donde
  las instancias de Sum se comunican. Por eso se descartó.

- **Un solo canal.** El problema anterior nacía de un problema en la falta de serialización
  de los mensajes por lo que otra solución explorada fue la de reutilizar la `input_queue`
  para ir difundiendo el EOF con un set marcando quienes fueron leyendo el mensaje y, mientras
  lo hacen flushean su información. Siendo que el código del gateway no podía modificarse
  y este enviaba los mensajes usando round-robin, no había forma de controlar quienes agarraban
  qué mensajes por lo que esta solución implicaba reencolar al final de la queue hasta que todos
  lean el mensaje en forma de pasamanos lo que agregaría una enorme latencia entre el tiempo
  donde los mensajes de un cliente ya fueron procesados por los sums y cuando estos terminaban
  de ser flusheados a la siguiente etapa del pipeline. Por ese mismo motivo se descartó también
  esta solución.

- **Serialización gracias al Middleware.** Parte del camino para llegar a la solución final
  fue intentar mantener la propiedad del canal único (FIFO global) sin tener que mezclar el flujo de
  control con el de datos, la idea era aprovecharse de la implementación de RabbitMQ que 
  cuenta con un solo executor **por channel** para usar este mismo para conectarse tanto
  a la `input_queue` como a un exchange de control de mediante la ampliación de la interfaz de middleware
  propuesta por la cátedra implementando `MessageMiddlewareMultiRabbitMQ`. La idea era que
  el uso de una sola conexión TCP con ese único executor funcione como embudo para la información
  otorgando la propiedad de serialización de mensajes y, aprovechándose del orden
  en el que se mandaban los mensajes conseguir evitar esta race condition. Fuera de si esta
  implementación fuera suficiente para evitar la race, o si solo estuviera cubriéndola aún
  más en la capa del middleware, esta implementación dependía fuertísimamente de cómo estuviera
  implementado el middleware. Un cambio de implementación sutil en esta pieza podía exponer
  la fragilidad del diseño reintroduciendo esta race silenciosamente así que antes de seguir
  evaluándola se decidió ir aún más profundo.

- **Conteo distribuido (camino elegido).** En lugar de *evitar* la race apoyándonos en
  el orden del transporte, la **toleramos** con una barrera por conteo que es correcta
  bajo cualquier orden de llegada. El gateway conoce **N**, la cantidad de mensajes de
  datos que envió por cliente: el `MessageHandler` los cuenta y lo incluye en el EOF,
  sin cambiar su interfaz pública. Cada Sum cuenta cuántos datos de ese cliente
  consumió y, una vez consumido el EOF, a través del *fanout* de control las instancias
  intercambian sus conteos parciales (un mapa `{sum_id: cantidad}`, donde cada una pisa su propia entrada).
  Cuando la suma de los conteos conocidos llega a N, cada Sum sabe que **entre todas**
  se consumieron todos los datos del cliente, y recién ahí vuelca su parcial. Como los
  conteos solo crecen y nunca superan N, que la suma llegue a N equivale a que no queda
  ningún dato en tránsito: el volcado prematuro es imposible sin importar el orden en
  que lleguen los mensajes, por lo que la correctitud ya no depende del orden en el que
  se envían los mensajes ni tampoco se reciben. Mientras la suma no llegue a N, cada instancia sigue consumiendo su parte y, por cada
  dato adicional del cliente que procesa, actualiza su conteo y lo vuelve a difundir por el
  exchange. Este reenvío por cada "delta" pendiente resulta barato porque el EOF es el
  **último** mensaje que emite el gateway: cuando una instancia lo lee, la gran mayoría de
  los datos del cliente ya fueron consumidos (o eso se asume), así que solo resta un puñado de mensajes en
  tránsito y, por lo tanto, muy pocas difusiones de conteo.

Se mantiene una sola conexión y un solo hilo por Sum (vía `MessageMiddlewareMultiRabbitMQ`,
que consume la work queue y el exchange fanout de control con un callback por fuente),
así que tampoco hay sección crítica ni lock. El volcado se evalúa localmente en cada
instancia apenas cambia algún conteo, sin bloquear el consumo.

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

- Cada **Aggregator** espera `SUM_AMOUNT` flushes de un cliente antes de calcular su
  top parcial. Como la partición es por fruta, un Aggregator puede recibir todos los
  flushes de un cliente del que no tiene ninguna fruta; en ese caso emite igualmente un
  parcial **vacío**, para no colgar la barrera del Join.
- El **Join** espera `AGGREGATION_AMOUNT` parciales por cliente, hace el merge y
  emite el top final. Un parcial vacío cuenta para la barrera aunque no aporte
  frutas.

El truncado a `TOP_SIZE` en el Aggregator es una optimización: como las frutas están
particionadas, una fruta que no entra al top local de su Aggregator tampoco puede
entrar al global, así que descartarla no pierde información. El Join solo mergea
tops disjuntos y reordena, por lo que el tráfico hacia él no escala por la cantidad
de frutas. Por lo pronto al momento de reordenar no se utiliza
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

Además, en el Sum el estado por cliente (conteos y acumulados) se descarta apenas se
vuelca su parcial, de modo que la memoria no tenga información sobre clientes cuyos 
datos ya fueron flusheados, sino que solamente se guarda el hecho que ya fueron procesados.

## Escalabilidad

- **Clientes:** el estado por `client_id` en cada etapa permite atender múltiples
  consultas concurrentes sin interferencia. Sumar clientes no requiere cambios.
- **Grandes volúmenes de datos:** el volumen se colapsa lo antes posible. El Sum
  agrega en memoria y envía un mensaje por fruta distinta, no por dato; el Aggregator
  trunca a `TOP_SIZE`. El canal de control transporta solo señales de coordinación
  (EOFs y conteos), nunca datos, así que su costo no crece con el volumen sino, a lo
  sumo, con la cantidad de instancias. Escalar el volumen se acompaña agregando
  instancias de Sum, que se reparten la ingesta por round-robin. El `prefetch=1` se usa
  para un reparto justo entre instancias; no es necesario para la correctitud (la
  barrera por conteo es válida con cualquier `prefetch`) sino para que todas las
  réplicas trabajen de forma __fair__.
- **Cantidad de controles:** subir `SUM_AMOUNT` o `AGGREGATION_AMOUNT` es una
  cuestión de configuración; las barreras se ajustan solas por env var y la partición
  por fruta redistribuye el trabajo entre los Aggregators disponibles.
