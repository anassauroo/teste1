import asyncio, json, time, uuid
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from typing import Dict
from settings import *
from models import Worker, WorkerCaps, Lease
from state import State
from matching import match_worker
import itertools

TOPIC_CANCEL_PREFIX = os.getenv("TOPIC_CANCEL_PREFIX", "jobs.cancel.")

state = State()
producer: AIOKafkaProducer

def job_topic(worker_id: str) -> str:
    return f"{TOPIC_PREFIX_JOBS}{worker_id}"

def default_requirements_for(stream: str) -> dict:
    # se existir override por stream, use:
    req = state.req_map.get(stream)
    if req: return req
    # fallback:
    return {
        "class": DEFAULT_PROC["class"],
        "kwargs": DEFAULT_PROC["kwargs"]
    }

async def send_job(stream: str, req_bundle: dict, wid: str):
    job_id = f"j-{uuid.uuid4().hex[:6]}"
    job = {
        "job_id": job_id,
        "stream": {"name": stream, "rtsp": f"rtsp://mediamtx:8554/{stream}"},
        "processor": {"class": req_bundle["class"], "kwargs": req_bundle["kwargs"]},
        "tracking": req_bundle.get("tracking", DEFAULT_TRACKING),
        "lease_ttl_sec": LEASE_TTL_SEC
    }
    state.workers[wid].running += 1
    state.leases[job_id] = Lease(job_id, wid, stream, time.time() + LEASE_TTL_SEC)
    await producer.send_and_wait(job_topic(wid), json.dumps(job).encode("utf-8"))


async def try_assign(stream: str):
    req_bundle = default_requirements_for(stream)
    wid = match_worker(req_bundle, state.workers)
    if wid:
        await send_job(stream, req_bundle, wid)
    else:
        state.enqueue(stream, req_bundle, priority=10)

async def assign_from_queue():
    item = state.pop_queue()
    if not item: return
    stream, req = item
    wid = match_worker(req, state.workers)
    if wid:
        await send_job(stream, req, wid)
    else:
        # recoloca no fim da fila
        state.enqueue(stream, req, priority=10)

async def lease_scavenger():
    while True:
        now = time.time()
        expired = [lid for lid, l in state.leases.items() if l.deadline < now]
        for lid in expired:
            l = state.leases.pop(lid)
            # reduz carga do worker e re-enfileira stream
            if l.worker_id in state.workers and state.workers[l.worker_id].running > 0:
                state.workers[l.worker_id].running -= 1
            state.enqueue(l.stream, default_requirements_for(l.stream), priority=5)  # prioridade maior
        # remove workers zumbis
        for wid, w in list(state.workers.items()):
            if now - w.last_seen > HEARTBEAT_TTL * 3:
                # marca indisponível: opcionalmente dropar
                pass
        await asyncio.sleep(2)

def _extract_stream_names(payload) -> list[str]:
    """
    Aceita formatos comuns de snapshot:
    - {"items":[{"name":"voo", ...}, ...]}
    - [{"name":"voo"}, {"name":"cam2"}]
    - ["voo","cam2"]
    - {"ativos":[...]} (fallback)
    """
    if isinstance(payload, dict):
        if "items" in payload and isinstance(payload["items"], list):
            return [it["name"] for it in payload["items"] if isinstance(it, dict) and "name" in it]
        if "ativos" in payload and isinstance(payload["ativos"], list):
            # tenta objetos com "name" ou strings
            names = []
            for it in payload["ativos"]:
                if isinstance(it, dict) and "name" in it:
                    names.append(it["name"])
                elif isinstance(it, str):
                    names.append(it)
            return names
        # se vier um objeto com chaves sendo nomes, pegue as chaves (menos comum)
        if all(isinstance(k, str) for k in payload.keys()):
            return list(payload.keys())
        return []
    if isinstance(payload, list):
        # lista de dicts com name, ou lista de strings
        names = []
        for it in payload:
            if isinstance(it, dict) and "name" in it:
                names.append(it["name"])
            elif isinstance(it, str):
                names.append(it)
        return names
    # fallback
    return []

async def _reconcile_after_snapshot():
    """Calcula diff e toma ações."""
    to_start, to_stop = state.streams_diff()

    # iniciar novos
    for s in to_start:
        await try_assign(s)

    # parar os que não estão mais abertos
    for s in to_stop:
        # encontre o lease/job para esse stream
        target = None
        for lid, l in list(state.leases.items()):
            if l.stream == s:
                target = (lid, l)
                break
        if not target:
            continue
        job_id, lease = target
        # opção A: enviar cancel (se o worker já suportar)
        await send_cancel(lease.worker_id, job_id, s)
        # opção B (fallback): só remover lease e deixar o worker terminar sozinho (ou expirar)
        # state.leases.pop(job_id, None)
        # if lease.worker_id in state.workers and state.workers[lease.worker_id].running > 0:
        #     state.workers[lease.worker_id].running -= 1

async def handle_streams():
    cons = AIOKafkaConsumer(
        TOPIC_STREAMS, bootstrap_servers=BROKER,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        group_id="dispatcher-snapshots"
    )
    await cons.start()
    try:
        async for msg in cons:
            payload = msg.value
            names = _extract_stream_names(payload)
            # atualiza snapshot e reconcilia
            state.update_open_streams(names)
            await _reconcile_after_snapshot()
    finally:
        await cons.stop()

async def handle_announce():
    cons = AIOKafkaConsumer(
        TOPIC_ANNOUNCE, bootstrap_servers=BROKER,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        group_id="dispatcher"
    )
    await cons.start()
    try:
        async for msg in cons:
            p = msg.value
            wid = p["worker_id"]
            caps = p["capabilities"]
            load = p.get("load", {"running":0})
            w = Worker(
                worker_id=wid,
                caps=WorkerCaps(
                    families=caps.get("families", []),
                    yolo_versions=caps.get("yolo_versions", []),
                    accel=caps.get("accel", []),
                    arch=caps.get("arch", "amd64"),
                    max_concurrency=int(caps.get("max_concurrency", 1))
                ),
                running=int(load.get("running", 0))
            )
            state.upsert_worker(w)
            state.mark_seen(wid)
            # sempre que aparece worker novo, tenta drenar fila
            await assign_from_queue()
    finally:
        await cons.stop()

async def handle_status():
    cons = AIOKafkaConsumer(
        TOPIC_STATUS, bootstrap_servers=BROKER,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        group_id="dispatcher"
    )
    await cons.start()
    try:
        async for msg in cons:
            p = msg.value
            wid = p.get("worker_id")
            if wid: state.mark_seen(wid)
            state_s = p.get("state")
            jid = p.get("job_id")
            if state_s == "running" and jid in state.leases:
                # renova lease
                state.leases[jid].deadline = time.time() + LEASE_TTL_SEC
            elif state_s in ("done", "error") and jid in state.leases:
                l = state.leases.pop(jid)
                if l.worker_id in state.workers and state.workers[l.worker_id].running > 0:
                    state.workers[l.worker_id].running -= 1
                # puxa próximo
                await assign_from_queue()
    finally:
        await cons.stop()

async def send_cancel(worker_id: str, job_id: str, stream: str):
    cancel = {"job_id": job_id, "stream": stream, "reason": "stream_closed"}
    await producer.send_and_wait(f"{TOPIC_CANCEL_PREFIX}{worker_id}", json.dumps(cancel).encode("utf-8"))


async def main():
    global producer
    producer = AIOKafkaProducer(bootstrap_servers=BROKER, linger_ms=0, acks=1)
    await producer.start()
    try:
        await asyncio.gather(
            handle_streams(),
            handle_announce(),
            handle_status(),
            lease_scavenger(),
        )
    finally:
        await producer.stop()

if __name__ == "__main__":
    asyncio.run(main())
