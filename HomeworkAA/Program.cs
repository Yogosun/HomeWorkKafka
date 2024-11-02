using HomeworkAA.Baskets;
using HomeworkAA.Events.Storage;
using HomeworkAA.Order;

namespace HomeworkAA
{
    using HomeworkAA.Menus;
    using System;

    class Program
    {
        static void Main()
        {
            var eventStore = new EventStore();
            var orderService = new OrderService(eventStore);
            var basket = new Basket();
            var menu = new Menu();

            while (true)
            {
                Console.WriteLine("1. Показать меню и добавить блюдо в корзину");
                Console.WriteLine("2. Удалить блюдо из корзины");
                Console.WriteLine("3. Посчитать стоимость корзины");
                Console.WriteLine("4. Создать заказ");
                Console.WriteLine("5. Проверить статус заказа");
                Console.WriteLine("6. Вывести список всех заказов");
                Console.WriteLine("7. Выйти");

                var choice = Console.ReadLine();

                switch (choice)
                {
                    case "1":
                        Console.WriteLine("Меню:");
                        foreach (var item in menu.Items)
                        {
                            Console.WriteLine($"{item.Id}. {item}"); // Выводим ID, название и цену
                        }

                        Console.Write("Введите номер блюда для добавления в корзину: ");
                        if (int.TryParse(Console.ReadLine(), out int itemId))
                        {
                            var menuItem = menu.GetMenuItemById(itemId);
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
                        else
                        {
                            Console.WriteLine("Неверный ввод.");
                        }
                        break;

                    case "2":
                        Console.Write("Введите название блюда для удаления из корзины: ");
                        var itemName = Console.ReadLine();
                        orderService.RemoveItemFromBasket(basket, itemName);
                        Console.WriteLine($"Блюдо {itemName} удалено из корзины.");
                        break;

                    case "3":
                        Console.WriteLine($"Стоимость корзины: {basket.CalculateTotal()} руб.");
                        break;

                    case "4":
                        var orderId = orderService.CreateOrder(basket);
                        Console.WriteLine($"Заказ {orderId} создан");
                        var simulator = new OrderProcessingSimulator(orderService, orderId);
                        simulator.Start();
                        break;

                    case "5":
                        Console.Write("Введите ID заказа: ");
                        if (int.TryParse(Console.ReadLine(), out orderId))
                        {
                            var status = orderService.GetOrderStatus(orderId);
                            Console.WriteLine(status.HasValue ? $"Статус заказа {orderId}: {status}" : "Заказ не найден");
                        }
                        else
                        {
                            Console.WriteLine("Неверный ввод.");
                        }
                        break;

                    case "6":
                        foreach (var order in orderService.ListOrders())
                        {
                            Console.WriteLine($"Заказ {order.OrderId}: {order.Status}");
                        }
                        break;

                    case "7":
                        return;

                    default:
                        Console.WriteLine("Неверный выбор");
                        break;
                }
            }
        }

    }
}
