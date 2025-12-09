# 🚀 Apache Kafka Exploration - Java R&D Project

A hands-on exploration of **Apache Kafka** fundamentals using **Java**, demonstrating producer-consumer patterns, message streaming, and event-driven architecture concepts.

---

## 📋 Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Getting Started](#getting-started)
- [Usage Examples](#usage-examples)
- [Key Concepts Explored](#key-concepts-explored)
- [Kafka UI](#kafka-ui)
- [Learning Outcomes](#learning-outcomes)

---

## 🎯 Overview

This R&D project explores Apache Kafka's core capabilities through practical Java implementations. It demonstrates fundamental messaging patterns including:

- **Producer-Consumer Architecture**: Basic message publishing and consumption
- **Keyed Messages**: Understanding partitioning and message ordering
- **Asynchronous Processing**: Callback-based message handling
- **Containerized Infrastructure**: Docker-based Kafka cluster setup

Perfect for developers learning distributed messaging systems or exploring event-driven architecture patterns.

---

## ✨ Features

### Producer Implementations

- ✅ **Basic Producer** - Simple message publishing without keys
- ✅ **Keyed Producer** - Partition-aware message routing using keys
- ✅ **Async Callbacks** - Non-blocking message delivery with acknowledgments
- ✅ **Batch Processing** - Multiple message production in loops

### Consumer Implementation

- ✅ **Continuous Polling** - Real-time message consumption
- ✅ **Consumer Groups** - Scalable message processing
- ✅ **Offset Tracking** - Message position monitoring
- ✅ **Partition Awareness** - Understanding message distribution

### Infrastructure

- ✅ **Docker Compose Setup** - One-command Kafka cluster deployment
- ✅ **Kafka UI Dashboard** - Visual monitoring and management
- ✅ **Zookeeper Integration** - Cluster coordination

---

## 🛠️ Tech Stack

| Technology       | Version | Purpose                            |
| ---------------- | ------- | ---------------------------------- |
| **Java**         | 17      | Core programming language          |
| **Apache Kafka** | 4.0.0   | Distributed streaming platform     |
| **Maven**        | -       | Dependency management & build tool |
| **Docker**       | -       | Containerization                   |
| **Kafka UI**     | Latest  | Web-based Kafka management         |
| **SLF4J**        | 2.0.17  | Logging framework                  |

---

## 📁 Project Structure

```
apche/
├── src/
│   └── main/
│       └── java/
│           ├── Producer/
│           │   ├── ProducerInIt.java       # Basic producer implementation
│           │   └── ProducerWithKey.java    # Keyed message producer
│           ├── Consumer/
│           │   └── ConsumerInIt.java       # Consumer implementation
│           └── com/apche/
│               └── Main.java               # Entry point
├── compose.yaml                            # Docker Compose configuration
├── pom.xml                                 # Maven dependencies
└── README.md                               # This file
```

---

## 🚀 Getting Started

### Prerequisites

- **Java 17** or higher
- **Maven** 3.6+
- **Docker** & **Docker Compose**

### Installation & Setup

1. **Clone the repository**

   ```bash
   git clone <repository-url>
   cd apche
   ```

2. **Start Kafka infrastructure**

   ```bash
   docker compose up -d
   ```

   This starts:

   - Zookeeper (port `2181`)
   - Kafka broker (ports `9092`, `29092`)
   - Kafka UI (port `8080`)

3. **Verify services are running**

   ```bash
   docker compose ps
   ```

4. **Build the project**
   ```bash
   mvn clean install
   ```

---

## 💡 Usage Examples

### Running the Basic Producer

Sends 10 messages to `Test_Topic` without keys:

```bash
mvn exec:java -Dexec.mainClass="Producer.ProducerInIt"
```

**Expected Output:**

```
✅ Sent to topic
Test_Topic partition
0 offset
42
```

### Running the Keyed Producer

Sends 10 keyed messages (ensures same key → same partition):

```bash
mvn exec:java -Dexec.mainClass="Producer.ProducerWithKey"
```

**Key Concept:** Messages with the same key always go to the same partition, maintaining order.

### Running the Consumer

Continuously polls and displays messages from `Test_Topic`:

```bash
mvn exec:java -Dexec.mainClass="Consumer.ConsumerInIt"
```

**Expected Output:**

```
KEY:ID_5, VALUE:VAL_5
Partitions:1, Offset:23
```

---

## 🧠 Key Concepts Explored

### 1. **Producer Patterns**

#### Without Keys (Round-Robin Distribution)

```java
ProducerRecord<String, String> record =
    new ProducerRecord<>("Test_Topic", "hello world");
```

- Messages distributed across partitions in round-robin fashion
- No ordering guarantees

#### With Keys (Partition Affinity)

```java
ProducerRecord<String, String> record =
    new ProducerRecord<>("Test_Topic", "ID_5", "VAL_5");
```

- Same key → same partition
- Ordering guaranteed within partition

### 2. **Asynchronous Callbacks**

```java
producer.send(record, (metadata, exception) -> {
    if (exception != null) {
        logger.info("❌ Error: " + exception.getMessage());
    } else {
        logger.info("✅ Sent to partition " + metadata.partition());
    }
});
```

### 3. **Consumer Groups**

- Multiple consumers can share workload
- Each partition consumed by only one consumer in a group
- Enables horizontal scaling

### 4. **Offset Management**

- Tracks message position in partition
- Enables replay and fault tolerance

---

## 🖥️ Kafka UI

Access the Kafka UI dashboard at **http://localhost:8080**

**Features:**

- 📊 View topics, partitions, and messages
- 🔍 Search and filter messages
- 📈 Monitor consumer lag
- ⚙️ Manage broker configurations

![Kafka UI Screenshot](https://kafka-ui.provectus.io/images/kafka-ui-light.png)

---

## 📚 Learning Outcomes

Through this project, I explored:

✅ **Kafka Architecture** - Brokers, topics, partitions, and replicas  
✅ **Producer API** - Synchronous vs asynchronous sending  
✅ **Consumer API** - Polling, offsets, and consumer groups  
✅ **Message Ordering** - Key-based partitioning strategies  
✅ **Docker Orchestration** - Multi-container Kafka setup  
✅ **Monitoring** - Using Kafka UI for operational insights

---

## 🔧 Configuration Details

### Kafka Broker Configuration

| Property                                 | Value                                                      | Description                   |
| ---------------------------------------- | ---------------------------------------------------------- | ----------------------------- |
| `KAFKA_BROKER_ID`                        | 1                                                          | Unique broker identifier      |
| `KAFKA_ZOOKEEPER_CONNECT`                | zookeeper:2181                                             | Zookeeper connection          |
| `KAFKA_ADVERTISED_LISTENERS`             | PLAINTEXT://kafka:9092<br>PLAINTEXT_HOST://localhost:29092 | Internal & external listeners |
| `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR` | 1                                                          | Offset topic replication      |

### Producer Configuration

```java
properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
```

### Consumer Configuration

```java
properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "earliest");
```

---

## 🛑 Stopping the Infrastructure

```bash
docker compose down
```

To remove volumes as well:

```bash
docker compose down -v
```

---

## 🤝 Contributing

This is an R&D project for learning purposes. Feel free to fork and experiment!

---

## 📝 License

This project is open source and available for educational purposes.

---

## 🔗 Useful Resources

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Kafka Java Client API](https://kafka.apache.org/documentation/#api)
- [Confluent Kafka Tutorials](https://kafka-tutorials.confluent.io/)
- [Kafka UI GitHub](https://github.com/provectus/kafka-ui)

---

**Built with ☕ and curiosity**
