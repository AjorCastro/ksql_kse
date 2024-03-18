# orquestador.py
import asyncio
import httpx
import signal
from datetime import datetime, timedelta
from modulo_asignacion import procesar_conversaciones_no_asignadas

KSQLDB_URL = "https://localhost:8088"
HEADERS = {"Content-Type": "application/vnd.ksql.v1+json; charset=utf-8"}


async def hacer_ping(client):
    payload_ping = {
        "ksql": "SELECT PINGME FROM ping_tb EMIT CHANGES LIMIT 1;",
        "streamsProperties": {"ksql.streams.auto.offset.reset": "latest"}
    }
    try:
        async with client.stream('POST', f"{KSQLDB_URL}/query-stream", json=payload_ping, headers=HEADERS) as response:
            async for chunk in response.aiter_lines():
                if chunk:
                    print("Ping exitoso para mantener la conexión activa.")
                    break

    except asyncio.CancelledError:
        print('Cancelled PINGME')


async def mantener_conexion_activa(client, intervalo_ping=300):
    try:
        while True:
            await asyncio.sleep(intervalo_ping)
            await hacer_ping(client)
    except asyncio.CancelledError:
        print('Mantener Conexion CancelledError')


async def main(client):

    try:
        tarea_mantener_conexion = asyncio.create_task(
            mantener_conexion_activa(client))
        tarea_procesar_conversaciones = asyncio.create_task(
            procesar_conversaciones_no_asignadas(client))

        await asyncio.gather(tarea_procesar_conversaciones, tarea_mantener_conexion)

    except asyncio.CancelledError:
        print('Main CancelledErro Except')


async def shutdown(loop, client):
    print("Deteniendo la aplicación...")
    try:
        # Cancelar todas las tareas pendientes excepto la tarea actual.
        tasks = [t for t in asyncio.all_tasks(
            loop) if t is not asyncio.current_task(loop)]
        for task in tasks:
            task.cancel()
        # Espera a que todas las tareas se cancelen.
        # print(f'TASKS: {task}')
        # await asyncio.gather(*tasks, return_exceptions=True)
        await asyncio.gather(*tasks)
        # Cerrar el cliente HTTP.
        await client.aclose()
        print("Cliente HTTP cerrado.")
        print("Aplicación detenida.")
    except asyncio.CancelledError:
        print('Shutdown CancelledError except.')


def get_shutdown_handler(client, loop):
    """Crea y devuelve un manejador de shutdown que incluye el cliente y el loop."""
    async def shutdown_handler():
        await shutdown(loop, client)
    return shutdown_handler


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
#    async with httpx.AsyncClient(http2=True, verify=False) as client:

    # Crear el cliente fuera de main para que sea accesible en shutdown
    client = httpx.AsyncClient(http2=True, verify=False)

    # Registrar manejadores de señal para SIGINT y SIGTERM.
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(
            sig, lambda: asyncio.ensure_future(get_shutdown_handler(client, loop)()))

    try:
        loop.run_until_complete(main(client))
    except asyncio.CancelledError:
        print('main')
    finally:
        # Garantizar cierre del cliente si se omite el shutdown
        loop.run_until_complete(client.aclose())
        loop.close()
