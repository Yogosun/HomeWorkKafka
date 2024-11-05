using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Avro.IO;
using Avro.Specific;
using Confluent.Kafka;
using Confluent.SchemaRegistry.Serdes;
using Confluent.SchemaRegistry;
using OrderProducer.OrderKafkaAvro;
using System.ComponentModel;

namespace OrderProducer
{

    public class OrderProducer
    {
        public static async Task Main()
        {
            string[] foodItems = { "Пицца", "Бургер", "Суши", "Паста", "Салат" };

            var producerConfig = new ProducerConfig
            {
                BootstrapServers = "localhost:9092"  // Укажите адрес Kafka-брокера
            };

            //Генерация и отправка заказов
            for (int i = 1; i <= 10; i++)
            {
                var order = new OrderKafka
                {
                    OrderId = Guid.NewGuid().ToString(),
                    CustomerName = $"Customer {i}",
                    Items = new List<string> { foodItems[new Random().Next(0, 4)], foodItems[new Random().Next(0, 4)], foodItems[new Random().Next(0, 4)] },
                    ReceivedTimestamp = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss")
                };

                // Сериализация объекта User в байты с помощью Avro
                byte[] avroData;
                using (var ms = new MemoryStream())
                {
                    var writer = new BinaryEncoder(ms);
                    var datumWriter = new SpecificDatumWriter<OrderKafka>(order.Schema);
                    datumWriter.Write(order, writer);
                    avroData = ms.ToArray();
                }

                try
                {
                    using (var producer = new ProducerBuilder<Null, byte[]>(producerConfig).Build())
                    {

                        var message = new Message<Null, byte[]>
                        {
                            Value = avroData
                        };

                        // Отправка сообщения в Kafka
                        var result = producer.ProduceAsync("orders", message).GetAwaiter().GetResult();
                        Console.WriteLine($"Order {order.OrderId} sent to partition {result.Partition}, offset {result.Offset}");

                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error producing message: {ex.Message}");
                }
                await Task.Delay(5000); // Пауза между отправками
            }
        }
    }
}
