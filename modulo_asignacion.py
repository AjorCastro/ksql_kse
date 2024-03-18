import httpx
import asyncio
import json
from datetime import datetime

KSQLDB_URL = "https://localhost:8088"
HEADERS = {"Content-Type": "application/vnd.ksql.v1+json; charset=utf-8"}


async def obtener_agentes_disponibles(client, biz_account_id, biz_phone_nbr_id):
    agente_query = {
        "sql": f"SELECT agente_id, nombre_agente FROM AGENTES_EMP_TB WHERE biz_account_id = '{biz_account_id}' AND biz_phone_nbr_id = '{biz_phone_nbr_id}' AND activo = 'activo';",
        "properties": {}
    }
    agentes = []
    try:
        async with client.stream('POST', f"{KSQLDB_URL}/query-stream", json=agente_query, headers=HEADERS) as response:
            es_primera_linea = True
            async for line in response.aiter_lines():
                if line.strip():
                    if es_primera_linea:
                        metadata = json.loads(line.strip(',\n'))
                        es_primera_linea = False
                    else:
                        data = json.loads(line.strip(',\n'))
                        agentes.append(
                            {"agente_id": data[0], "nombre_agente": data[1]})
    except asyncio.CancelledError:
        print("La tarea Busca Agentes esta concluida.")
    return agentes


async def obtener_carga_trabajo_agente(client, agente_id):
    query_carga = {
        "sql": f"SELECT tot_convos_asigned FROM WORKLOAD_AGENTES_TB WHERE agente_id = '{agente_id}';",
        "properties": {}
    }

    carga_trabajo = 0
    try:
        async with client.stream('POST', f"{KSQLDB_URL}/query-stream", json=query_carga, headers=HEADERS) as response:
            es_primera_linea = True
            async for line in response.aiter_lines():
                if line.strip():
                    if es_primera_linea:
                        metadata = json.loads(line.strip(',\n'))
                        es_primera_linea = False
                    else:
                        data = json.loads(line.strip(',\n'))
                        carga_trabajo = data[0]

    except asyncio.CancelledError:
        print("La workload esta concluida.")
    return (agente_id, carga_trabajo)


async def asignar_conversacion_a_agente(client, convo_key, agente_id):
    now = datetime.utcnow()
    unix_timestamp = now.timestamp()
    timestamp_str = str(int(unix_timestamp))
    payload = {"target": "ASIGNACIONES_ST"}
    valores = json.dumps(
        {"convo_key": convo_key, "agente_id": agente_id, "timestamp": timestamp_str}) + "\n"
    data_to_send = json.dumps(payload) + "\n" + valores
    data_to_send_utf8 = data_to_send.encode('utf-8')
    # print(f'Data To send : {data_to_send}')
    # print(f'Data To send UTF-8: {data_to_send_utf8}')

    try:
        async with client.stream("POST", f"{KSQLDB_URL}/inserts-stream", headers=HEADERS, data=data_to_send_utf8) as response:
            print(response.http_version)
            async for line in response.aiter_lines():
                ack = json.loads(line.strip())
                print(f"ACK: {ack}")
                break
    except asyncio.CancelledError:
        print("La ASIGNACIONES esta concluida.")


async def procesar_conversaciones_no_asignadas(client):
    QUERY = {
        "sql": "SELECT * FROM CONVO_NO_ASIGNADA_TB EMIT CHANGES;",
        "properties": {"ksql.streams.auto.offset.reset": "earliest"}
    }
    try:
        async with client.stream('POST', f"{KSQLDB_URL}/query-stream", json=QUERY, headers=HEADERS, timeout=None) as response:
            es_primera_linea = True
            async for line in response.aiter_lines():
                if line.strip():
                    if es_primera_linea:
                        # Procesa la primera línea (metadata)
                        # metadata = json.loads(line.decode('utf-8').strip(',\n'))  YA viene decodificada por httpx
                        metadata = line
                        print(
                            f"Metadata recibida DE CONVO_NO_ASIGNADA_TB: {metadata}")
                        es_primera_linea = False
                    else:
                        # Procesa las siguientes líneas (datos)
                        # data = json.loads(line.decode('utf-8').strip(',\n'))
                        data = json.loads(line)
                        # Ejemplo: ['261064770412999-227568487111999-56987620903', 'no asignada']
                        print(
                            f"Tipo de data: {type(data)}, Contenido de data: {data}")
                        convo_key = data[0]
                        print(f'convo_key: {convo_key}')
                        biz_account_id, biz_phone_nbr_id, _ = convo_key.split(
                            '-')
                        print(
                            f'biz_account_id: {biz_account_id} - biz_phone_nbr_id: {biz_phone_nbr_id}')
                        agentes = await obtener_agentes_disponibles(client, biz_account_id, biz_phone_nbr_id)
                        if not agentes:
                            print(
                                f"No se encontraron agentes disponibles para: {biz_account_id}, {biz_phone_nbr_id}")
                            continue
                        print(f'Agentes encontrados: {agentes}')
                        cargas = []
                        # cargas = await asyncio.gather(*(obtener_carga_trabajo_agente(session, agente['agente_id']) for agente in agentes))
                        cargas = await asyncio.gather(*(obtener_carga_trabajo_agente(client, agente['agente_id']) for agente in agentes),
                                                      return_exceptions=True
                                                      )
                        print(f'Cargas: {cargas}')
                        # Filtrar las cargas para excluir posibles excepciones si usas return_exceptions=True
                        cargas_filtradas = [
                            carga for carga in cargas if not isinstance(carga, Exception)]

                        # Encontrar el agente con la menor carga
                        # Asegúrate de que cargas_filtradas no esté vacío antes de hacer esto
                        if cargas_filtradas:
                            agente_con_menor_carga = min(
                                cargas_filtradas, key=lambda x: x[1])[0]
                            print(
                                f'Agente con menor carga: {agente_con_menor_carga}')
                        else:
                            print(
                                "No se encontraron cargas válidas o todos los intentos resultaron en excepciones.")

                        await asignar_conversacion_a_agente(client, convo_key, agente_con_menor_carga)
                        print(
                            f"Conversación {convo_key} asignada a {agente_con_menor_carga}")

    except asyncio.CancelledError:
        # await client.aclose()
        print("La tarea de procesar conversaciones no asignadas fue cancelada.")
