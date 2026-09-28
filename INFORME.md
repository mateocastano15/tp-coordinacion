# Informe

## Separación de clientes

El `MessageHandler` del gateway genera un id (uuid) por cada conexión. Todos los mensajes internos tienen la forma `[tipo, client_id, ...]`, y cada nodo guarda su estado en diccionarios indexados por cliente, que se liberan cuando ese cliente termina. Así varios clientes pueden procesarse en paralelo sin mezclarse. Al volver, el handler solo acepta el `TOP` cuyo id coincide con el suyo.

Tipos de mensaje:

- `DATA`: `[DATA, client_id, fruta, cantidad]`
- `EOF`: fin de datos de un cliente. Desde el gateway lleva el total de registros enviados.
- `LATE_EOF`: registros que un Sum procesó después de haber cerrado a ese cliente.
- `TOP`: top parcial o final de un cliente.

## Coordinación entre instancias de Sum

Las instancias de Sum consumen de la misma cola, así que cada registro lo procesa una sola. Cada una acumula un total por cliente y por fruta.

El EOF de un cliente también lo recibe una sola instancia. Esa instancia no envía nada: reenvía el EOF por `SUM_CONTROL_EXCHANGE` a todas las instancias de Sum, incluida ella misma, con una routing key por instancia. Cada Sum consume ese exchange en un segundo hilo. Al recibir el EOF envía sus parciales del cliente a los Aggregators y después un `EOF` con la cantidad de registros que procesó.

Como los datos y el control llegan por canales distintos, puede pasar que un Sum procese un dato de un cliente después de haberlo cerrado. Por eso el gateway incluye en el EOF el total de registros enviados. Si un Sum procesa un dato de un cliente ya cerrado, lo envía en el momento junto con un `LATE_EOF` que informa ese registro extra. Puede haber más de un `LATE_EOF` por cliente, uno por cada Sum que haya quedado con un dato pendiente. El Aggregator simplemente suma los registros informados hasta llegar al total. Como `prefetch_count` es 1, a cada Sum le puede quedar como mucho un mensaje en vuelo, así que después de procesar un mensaje más ya puede olvidar al cliente.

## Coordinación entre Sum y Aggregation

Cada Sum envía cada fruta a un único Aggregator según `crc32(fruta) % AGGREGATION_AMOUNT`. Todos los parciales de una misma fruta terminan en el mismo Aggregator y no hay procesamiento repetido.

Los `EOF` y `LATE_EOF` se envían a todos los Aggregators. Un Aggregator cierra un cliente cuando se cumplen dos condiciones:

1. Recibió `SUM_AMOUNT` EOFs, uno por Sum.
2. La suma de registros informados llegó al total que mandó el gateway.

Recién ahí envía su top parcial al Join. El Join espera `AGGREGATION_AMOUNT` tops por cliente y arma el top final. Como cada Aggregator tiene frutas distintas, el top global está entre los tops parciales.

## Escalabilidad

- **Clientes:** el estado está separado por `client_id` y se libera al terminar cada cliente, así que los clientes se procesan de forma concurrente sin interferir. El Join es una única instancia, pero su trabajo por cliente es mínimo: `AGGREGATION_AMOUNT` tops de `TOP_SIZE` elementos.
- **Volumen de datos:** un Sum guarda un total por fruta y por cliente. Su memoria depende de la cantidad de frutas distintas y no de la cantidad de registros. Además, los Aggregators reciben parciales ya sumados y no cada registro.
- **Cantidad de controles:**
  - Todo se parametriza con `SUM_AMOUNT` y `AGGREGATION_AMOUNT`.
  - Agregar Sums reparte la ingesta, con un costo de un EOF extra por Sum hacia cada Aggregator.
  - Agregar Aggregators reparte las frutas.
  - El balance entre Aggregators depende de cómo se distribuyan las frutas: una fruta con muchos registros cae siempre en el mismo Aggregator.

## Manejo de SIGTERM

Cada nodo registra un handler que detiene el consumo y cierra todas sus conexiones en un `finally`. En el middleware, `stop_consuming` usa `add_callback_threadsafe` de pika, para poder llamarlo desde el handler o desde otro hilo sin interferir con el loop de consumo. Si hay un mensaje en proceso, se termina de procesar antes de cortar.

En Sum cada hilo cierra sus propias conexiones. El hilo principal detiene al de control, lo espera y después cierra las suyas.
