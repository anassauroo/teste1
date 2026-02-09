from kafka import KafkaProducer
import sys

# Configuração
TOPIC = "teste"
BROKER = "localhost:29092"  # ajuste se necessário

# Criar producer Kafka
producer = KafkaProducer(
    bootstrap_servers=BROKER,
    value_serializer=lambda x: x.encode('utf-8')
)

print(f"✉️ Enviando mensagens para o tópico '{TOPIC}' em {BROKER}")
print("Digite mensagens e pressione Enter. Ctrl+C para sair.\n")

try:
    for line in sys.stdin:
        message = line.strip()
        if message:
            producer.send(TOPIC, value=message)
            producer.flush()
            print(f"✅ Enviado: {message}")
except KeyboardInterrupt:
    print("\nEncerrando...")
finally:
    producer.flush()
    producer.close()