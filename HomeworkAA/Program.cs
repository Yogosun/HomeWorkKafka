using HomeworkAA.Baskets;
using HomeworkAA.Events.Storage;
using HomeworkAA.Order;

namespace HomeworkAA
{
    using HomeworkAA.Menus;
    using System;
    using System.IO;
    using System.Threading;
    using Avro.IO;
    using Avro.Specific;
    using Confluent.Kafka;
    using HomeworkAA.OrderKafkaAvro;
    using Confluent.SchemaRegistry;
    using Confluent.SchemaRegistry.Serdes;
    using Confluent.Kafka.SyncOverAsync;

    class Program
    {
        static void Main()
        {
            var eventStore = new EventStore();
            var orderService = new OrderService(eventStore);
            var basket = new Basket();
            var menu = new Menu();

            // Конфигурация Consumer для чтения сообщений из Kafka
            var consumerConfig = new ConsumerConfig
            {
                GroupId = "user-consumer-group",  // Уникальная группа потребителей
                BootstrapServers = "localhost:9092",  // Адрес Kafka брокера
                AutoOffsetReset = AutoOffsetReset.Earliest,  // Начало чтения с самого раннего сообщения
                EnableAutoCommit = false  // Отключаем автоматическое коммитирование смещений
            };

            // Создание Consumer для получения сообщений
            var consumer = new ConsumerBuilder<Ignore, byte[]>(consumerConfig).Build();

            consumer.Subscribe("orders");  // Подписка на топик

            var cancellationToken = new CancellationTokenSource();

            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true; // Завершение при нажатии Ctrl+C
                cancellationToken.Cancel();
            };

            Console.WriteLine("Waiting for messages...");
            try
            {

                while (!cancellationToken.Token.IsCancellationRequested)
                {
                    try
                    {
                        var consumeResult = consumer.Consume(cancellationToken.Token);
                        var order = DeserializeAvroMessage(consumeResult.Message.Value);
                        DateTime.TryParse(order.ReceivedTimestamp, out DateTime sendTimestamp);
                        // Обработка заказа
                        Console.WriteLine($"Processing order: {order.OrderId}, Customer: {order.CustomerName}, Total: {order.TotalAmount}, SentTimestamp: {sendTimestamp}, ReceivedTimestamp {DateTime.UtcNow}");

                        foreach (var item in order.Items)
                        {
                            Console.WriteLine($"{item}"); // Выводим название

                            var menuItem = menu.GetMenuItemByName(item);
                            if (menuItem != null)
                            {
                                orderService.AddItemToBasket(basket, menuItem);
                                Console.WriteLine($"Добавлено в корзину: {menuItem.Name} - {menuItem.Price} руб.");
                            }
                            else
                            {
                                Console.WriteLine("Блюдо с таким номером не найдено.");
                            }
                        }

                        Console.WriteLine($"Стоимость корзины: {basket.CalculateTotal()} руб.");

                        var orderId = orderService.CreateOrder(basket);
                        Console.WriteLine($"Заказ {orderId} создан");
                        var simulator = new OrderProcessingSimulator(orderService, orderId);
                        simulator.Start();

                        // Ручное подтверждение обработки сообщения
                        consumer.Commit(consumeResult);
                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine($"Error consuming message: {e.Error.Reason}");
                    }
                }
            }

            catch (OperationCanceledException)
            {
                Console.WriteLine("Consumer stopped.");
            }
            finally
            {
                consumer.Close();
            }
        }

        // Метод для десериализации Avro-сообщения
        static OrderKafka DeserializeAvroMessage(byte[] avroData)
        {
            // Определяем схему User
            var orderSchema = OrderKafka._SCHEMA;

            // Десериализация Avro-сообщения из двоичных данных
            using (var stream = new MemoryStream(avroData))
            {
                var reader = new BinaryDecoder(stream);
                var datumReader = new SpecificDatumReader<OrderKafka>(orderSchema, orderSchema);

                // Чтение и десериализация данных
                return datumReader.Read(null, reader);
            }
        }
    }
}
