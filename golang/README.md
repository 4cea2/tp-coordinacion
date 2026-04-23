## Coordinación en Sum

Las instancias de Sum se coordinan mediante un exchange (`sumPrefix`) de control con una misma key (`broadcast`).
Cada instancia mantiene el estado parcial por cliente en `clientFruits`. 

Cada Sum ejecuta dos hilos:
- Uno consumiendo de inputQueue
- Otro consumiendo del exchange de control (exchangeSums)

### Flujo de coordinacion
- Al recibir un **EOF** desde `inputQueue`:
    1. Publica un **EOF de control** en el exchange
    2. Hace *flush* de su estado local del cliente hacia **Aggregation**
    3. Envía un **EOF** a todas las instancias de Aggregation
- Al recibir un **EOF de control** desde el exchange:
    1. Ejecuta el mismo flujo, excepto que no vuelve a publicar un nuevo EOF de control (paso i)

### Race condition entre los hilos
Ademas de coordinar las instancias de Sum, tambien se coordinan los 2 hilos internos.

Puede pasar que:
- Se procese primero un EOF del exchange
- Pero todavia quedan mensajes pendientes en inputQueue para ese mismo cliente

En ese caso se pierde informacion del cliente, porque el EOF se procesaria antes que los datos pendientes del cliente.

Para solucionar esto, se usa un mutex para garantizar el orden del acceso:
- Un lock en `handleMessage` (procesamiento inputQueue)
- Otro lock en el procesamiento de los EOFs del exchange de control

Con esto se asegura que se procesen primero los mensajes pendientes del cliente (si es que esta procesando), y luego el EOF, evitando asi la perdida de informacion del cliente.

Ademas, se hace uso del `prefetch = 1` en el inputQueue:
- Cada instancia tenga como maximo un mensaje sin confirmar
- Se procesa de a uno, evitando la acumulacion de mensajes en vuelo

---

## Coordinación en Aggregation
No se requirio mucha coordinacion de los aggregations, ya que cada instancia recibe un EOF de cada instancia de Sum. Una vez que le llegan todos los EOF de cada cliente por cada sum, 
envia un top parcial al Join, el cual se encargara de hacer el top global final.

Entonces, para cada instancia de los Aggregations:
1. Mantiene un contador de EOFs recibidos por cliente
2. Cuando el contador de EOFs por cliente llega a la cantidad de sums, se considera que el cliente no enviara mas data
3. Se calcula el top local y se envia a Join, junto con un EOF
---
## Join
Cuenta con un contador de EOFs recibidos por cliente, y siempre armara y enviara el top final cuando todas las instancias de Aggregations hayan enviado su EOF por cliente.


## Escalado respecto a clientes
Entiendo que, al aumentar/disminuir la cantidad de clientes no afecta en como se distribuye la carga entre las instancias 
de Sum, ya que cada dato del cliente puede ser procesado por cualquier Sum.

Esto tambien aplica para los Aggregations, ya que cada instancia de Sum envia sus datos locales a una instancia especifica de Aggregation en funcion del nombre de la fruta y el id del cliente, es decir:
- (fruta, cliente) -> instancia de Aggregation

Con esto, el mismo par siempre va a parar a la misma instancia de Aggregation, permitiendo el balanceo de carga "razonable".

Eso si, un problema aparece si un cliente envia siempre la misma fruta. Toda la carga cae en una sola instancia de Aggregation.
Hay pocas probabilidades de que ocurra, pero puede pasar.

Llegado al caso, tendria que buscar una mejor manera para repartir los datos de los clients entre los Aggregations, que ya no sean solo el par fruta-cliente, sino por un factor mas que me impida caer en la situacion anterior, capaz agregarle el factor cantidad de frutas por ejemplo.

## Costo de control (EOF)

Mientras mas clientes haya, mas EOFs de control se generaran entre los Sums, Aggregations y el Join para mantener la coordinacion.

Por cada cliente:
1. Genera un EOF de entrada:
   - 1 broadcast en Sum
   - Entregado a **S** instancias de Sum
   - Total:
     ```
     S EOFs entre los Sums
     ```

2. Cada instancia de Sum:
   - Envía EOF a **A** instancias de Aggregation
   - Total:
       ```
       (S * A) EOFs entre los Aggregations
       ```

3. Cada Aggregation:
   - Envía 1 EOF a Join
   - Total:
       ```
       A EOFs hacia Join
       ```

## Prefetch = 1

El prefetch de 1 en el inputQueue de Sum garantiza que el mismo pueda tener como maximo 1 mensaje sin confirmar en vuelo, evitando el caso borde de procesar un EOF del exchange antes de procesar los mensajes que tiene en vuelo cierto Sum.

Sin el prefetch de 1:
1. Un Sum puede tener bastantes mensajes en vuelo, en donde cualquiera de ellos puede tener datos de un cliente.
2. Si llega un EOF del exchange de control, el Sum tendria que procesar los mensajes antes de procesar el EOF. 

El problema esta es que no puedo saber de antemano si entre esos mensajes de vuelo se encuentra la data del cliente del EOF que llego.

Para ponerlo en un ejemplo por como yo entiendo esto:
- Supongamos que el Sum tiene 3 mensajes en vuelo, cada uno con datos del cliente A, B y C respectivamente
- Esta procesando el mensaje de C, una vez que termine procesará el de B y luego A
- Pero si llega un EOF perteneciente a A del exchange de control mientras esta procesando el C, el Sum tendria que procesar los 3 mensajes antes de procesar el EOF.

Entiendo que esto baja bastante la perfomance del sistema, ya que RabbitMQ no enviara mas mensajes a un Sum hasta recibir su ack, por ende lo enviara a la siguiente instancia que no este ocupado, provocando asi que la cola de espera pueda llenarse (cuello de botella).