# MatCom Messenger

> Profesor encargado: Lic. Fernando Martínez González [frndmartinezglez@gmail.com](mailto:frndmartinezglez@gmail.com)

## Introducción

Los mensajería tiene un importante papel en nuestros días. Empezando con los correos electrónicos hasta las aplicaciones de mensajes más completas. Se quisiera además que el servicio de mensajería no dependiera de un ente centralizado que regula los perfiles y potencialmente “lee” nuestras conversaciones. En lugar de esto se desea proveer de una arquitectura descentralizada y segura, en la que, cualquiera pueda recibir y mandar mensajes con la garantía de que solo lo podrán ver los involucrados en la conversación.

## Especificaciones

Existen dos entidades fundamentales en la realización del proyecto: cliente y gestor de entidades. Los clientes son el sistema en sí y la comunicación entre estos es la piedra angular de la tarea, usted está encargado de definir los protocolos y los algoritmos a utilizar. Los gestores de identidades son los encargados de manejar la información de los usuarios. Cualquier modificación o aclaración ver al profesor encargado de la orientación.

### Cliente

El cliente es la entidad encargada de mandar y recibir mensajes. La primera vez que un usuario se añade a la red, debe identificarse para luego adicionar contactos y crear chats. La información de las conversaciones se almacena en los clientes y no puede suceder que si el destinatario no está conectado al sistema el remitente no pueda mandar el mensaje. La comunicación debe poderse realizar en todo momento sin que se utilice un servicio diferente para el almacenamiento. Esta funcionalidad debe recaer completamente en los clientes. 

La implementación de este cliente debe venir acompañada con una aplicación gráfica que permita una mejor interacción con el sistema.

### Gestor de identidades

Un gestor de identidades es un servicio en el que los usuarios crean sus cuentas para guardar información que luego puede ser consumida por otras aplicaciones. Esta información solo puede ser consultada por el propio usuario, o por lo aplicación previa consulta al mismo. Este elemento facilita que los usuarios finales puedan utilizar un servicio como este sin preocuparse por las interioridades del sistema.

Este elemento no puede representar un punto de falla única en su sistema.

## Recomendaciones

Existen varias maneras de enfocar este proyecto. Los requerimientos son los especificados. Los mecanismos que se utilicen serán parte de la evaluación (¿por qué?, ventajas y desventajas, etc). Ideas útiles pueden ser las de blockchain y bitttorrent.

Sobre los aspectos que siempre se tendrán en cuenta en la revisión son:

* tolerancia a fallas
* replicación.

El soporte de estas debe ser, preferentemente, parametrizado, para que el servicio ofrecido pueda escalar sin dificultad.

El trabajo a realizar es abundante, por este motivo es de vital importancia las buenas prácticas de programación. Conocimientos de virtualización, testing y patrones de diseño, pueden ayudarlo a franquear este desafío. Trazar metas pequeñas, que progresivamente estructuren todo el proyecto, resulta vital, para esto pueden utilizar un gestor de tareas (Trello, Issues de Gitlab, etc).

## Evaluación

Todos los proyectos poseen un alto contenido ingenieril. Si durante la realización del proyecto, el estudiante utiliza alguna herramienta de evaluación automática (testing), se incentiva a que lo comente y lo utilice durante la revisión.

La calificación final del proyecto es resultado de su trabajo y del desenvolvimiento del equipo durante la exposición. Presentar un informe escrito, si bien no es obligatorio, será bien considerado en el momento de evaluar.
