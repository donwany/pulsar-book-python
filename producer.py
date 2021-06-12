import pulsar


TOPIC = 'my-topic'
client = pulsar.Client('pulsar://localhost:6650')

producer = client.create_producer(TOPIC)

data = [
    {"age": 20, "school":"MSU", "height": 170},
    {"age": 50, "school":"MSU", "height": 200},
    {"age": 60, "school":"MSU", "height": 175}
]
for i in data:
    producer.send(('%s' % i).encode('utf-8'))

producer.send("I am just trying out pulsar for the first time".encode('utf-8'))


if __name__ == '__main__':
    producer.flush()
    client.close()