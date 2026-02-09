from fastapi import FastAPI, Form
import json, requests
import os
import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from fastapi.responses import JSONResponse
import sqlite3
import httpx
from datetime import datetime

app = FastAPI()

# Configuração por variáveis de ambiente
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC = os.getenv("TOPIC", "streams_ativos")

# URL da API do MediaMTX
MEDIAMTX_API = "http://mediamtx:8888/v1/path/list"

def wait_for_kafka(bootstrap_servers: str, retries: int = 20, delay: int = 3):
    for attempt in range(retries):
        try:
            print(f"[Tentativa {attempt+1}] Conectando ao Kafka em {bootstrap_servers}...")
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
            print("✅ Kafka conectado com sucesso!")
            return producer
        except NoBrokersAvailable as e:
            print(f"⚠️ Kafka ainda indisponível: {e}. Tentando novamente em {delay}s...")
            time.sleep(delay)
    raise RuntimeError("❌ Não foi possível conectar ao Kafka após várias tentativas.")

# Exemplo de uso:
producer = wait_for_kafka("kafka:9092")

def inicializar_banco():
    conn = sqlite3.connect("streams.db")
    c = conn.cursor()
    print ("Saving connection")
    # Criação da tabela com colunas específicas
    c.execute("""
        CREATE TABLE IF NOT EXISTS ativos (
            id TEXT PRIMARY KEY,
            nome TEXT,
            config TEXT,
            source_type TEXT,
            ready_time TEXT,
            leitores INTEGER,
            atualizado_em TEXT
        )
    """)
    c.execute("""
        DELETE FROM ativos
    """)
    conn.commit()
    conn.close()

def salvar_em_banco(lista_streams):
    if len(lista_streams)==0:
        return
    conn = sqlite3.connect("streams.db")
    c = conn.cursor()
    print ("Saving connection")

    for stream in lista_streams:
        c.execute("""
            INSERT OR REPLACE INTO ativos (id, nome, config, source_type, ready_time, leitores, atualizado_em)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            stream.get("source_id"),
            stream.get("nome"),
            stream.get("config"),
            stream.get("source_type"),
            stream.get("ready_time"),
            stream.get("leitores"),
            stream.get("atualizado_em")
        ))

    conn.commit()
    conn.close()

def publicar_kafka(lista_streams):
    producer.send("streams_ativos", {"streams": lista_streams, "timestamp": time.time()})

def atualizar_estado():
    try:
        res = requests.get(MEDIAMTX_API)
        paths = res.json().get("items", [])
        lista = [p["name"] for p in paths]
        salvar_em_banco(lista)
        publicar_kafka(lista)
        print("Atualizado com sucesso:", lista)
    except Exception as e:
        print("Erro ao atualizar estado:", e)


async def atualizar_streamings_ativos():
    try:
        print("atualizar tabela")
        async with httpx.AsyncClient() as client:
            resp = await client.get("http://mediamtx:9997/v3/paths/list", timeout=5.0)

        if resp.status_code != 200:
            print("Erro ao consultar MediaMTX:", resp.status_code, resp.text)
            return



        data = resp.json()
        stream_infos=[]
        print(json.dumps(data, indent=2))
        inicializar_banco()
        if len(data.get("items", [])) != 0:
            for item in data.get("items", []):
                stream_info = {
                    "nome": item.get("name"),
                    "config": item.get("confName"),
                    "source_type": item.get("source", {}).get("type"),
                    "source_id": item.get("source", {}).get("id"),
                    "ready": item.get("ready"),
                    "ready_time": item.get("readyTime"),
                    "leitores": len(item.get("readers", [])),
                    "atualizado_em": datetime.utcnow().isoformat()
                }
                stream_infos.append(stream_info)

  
        # Salvar no banco de dados (ex: MongoDB ou SQL)
        salvar_em_banco(stream_infos)
        print("salvo db")
        # Publicar no Kafka
        publicar_kafka(stream_infos)

        print("Atualização de streams concluída")

    except Exception as e:
        print("Erro ao atualizar streams:", e)


@app.post("/evento")
async def receber_evento(
    stream: str = Form(...),
    tipo: str = Form("connect")
):
    evento = {
        "stream": stream,
        "tipo": tipo
    }
    print("Evento recebido:", evento)
    await atualizar_streamings_ativos()
    return {"status": "publicado"}

@app.get("/ativos")
async def get_streamings_ativos():
    try:
        await atualizar_streamings_ativos()
        print("get ativos")
        conn = sqlite3.connect("streams.db")
        conn.row_factory = sqlite3.Row  # Permite acessar os dados como dicionário
        c = conn.cursor()
        c.execute("SELECT * FROM ativos")
        rows = c.fetchall()
        conn.close()

        ativos = [dict(row) for row in rows]
        return JSONResponse(content={"ativos": ativos})

    except Exception as e:
        return JSONResponse(status_code=500, content={"erro2": str(e)})