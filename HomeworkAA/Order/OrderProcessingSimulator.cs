using HomeworkAA.Entity;
using System.Threading;

namespace HomeworkAA.Order
{
    public class OrderProcessingSimulator
    {
        private readonly OrderService _orderService;
        private readonly int _orderId;
        private readonly OrderStatus[] _statusSequence =
            { OrderStatus.Processing, OrderStatus.OnTheWay, OrderStatus.Delivered };

        public OrderProcessingSimulator(OrderService orderService, int orderId)
        {
            _orderService = orderService;
            _orderId = orderId;
        }

        public void Start()
        {
            new Thread(() =>
            {
                foreach (var status in _statusSequence)
                {
                    Thread.Sleep(5000); // Пауза в 5 секунд перед обновлением статуса
                    _orderService.UpdateOrderStatus(_orderId, status);
                }
            }).Start();
        }
    }
}
