using HomeworkAA.Baskets;
using HomeworkAA.Entity;
using HomeworkAA.Events.Basket;
using HomeworkAA.Events.Order;
using HomeworkAA.Events.Storage;
using System.Collections.Generic;

namespace HomeworkAA.Order
{
    public class OrderService
    {
        private readonly EventStore _eventStore;
        private readonly Dictionary<int, Order> _orders = new Dictionary<int, Order>();
        private int _orderIdCounter = 1;

        public OrderService(EventStore eventStore)
        {
            _eventStore = eventStore;
        }

        public void AddItemToBasket(Basket basket, MenuItem menuItem)
        {
            basket.AddItem(menuItem.Name, menuItem.Price);
            _eventStore.SaveEvent(new ItemAddedEvent(menuItem.Name, menuItem.Price));
        }

        public void RemoveItemFromBasket(Basket basket, string item)
        {
            basket.RemoveItem(item);
            _eventStore.SaveEvent(new ItemRemovedEvent(item));
        }

        public int CreateOrder(Basket basket)
        {
            var order = new Order(_orderIdCounter++, basket);
            _orders[order.OrderId] = order;
            _eventStore.SaveEvent(new OrderStatusChangedEvent(order.OrderId, OrderStatus.Created));

            basket.Clear();

            return order.OrderId;
        }

        public void UpdateOrderStatus(int orderId, OrderStatus status)
        {
            if (_orders.TryGetValue(orderId, out var order))
            {
                order.ChangeStatus(status);
                _eventStore.SaveEvent(new OrderStatusChangedEvent(orderId, status));
            }
        }

        public OrderStatus? GetOrderStatus(int orderId)
        {
            return _orders.TryGetValue(orderId, out var order) ? order.Status : (OrderStatus?)null;
        }

        public IReadOnlyList<(int OrderId, OrderStatus Status)> ListOrders()
        {
            var orderList = new List<(int OrderId, OrderStatus Status)>();
            foreach (var order in _orders)
            {
                orderList.Add((order.Key, order.Value.Status));
            }
            return orderList;
        }
    }
}
