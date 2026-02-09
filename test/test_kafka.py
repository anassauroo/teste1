from kafka import KafkaConsumer

# Configurações básicas
TOPIC = "results.detections"
BROKER = "localhost:29092"  # ou "localhost:29092" se estiver rodando localmente

# Criar consumidor Kafka
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[BROKER],
    auto_offset_reset='latest',  # ou 'latest' para começar pelo final
    enable_auto_commit=True,
    group_id="test-group3",         # pode ser qualquer string
    value_deserializer=lambda x: x.decode('utf-8')
)

print(f"📡 Escutando tópico '{TOPIC}' em {BROKER}...")

# Loop de escuta
for message in consumer:
    print(f"[Kafka] Mensagem recebida: {message.value}")