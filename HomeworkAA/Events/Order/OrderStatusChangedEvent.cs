using HomeworkAA.Entity;

namespace HomeworkAA.Events.Order
{
    public class OrderStatusChangedEvent
    {
        public int OrderId { get; }
        public OrderStatus Status { get; }

        public OrderStatusChangedEvent(int orderId, OrderStatus status)
        {
            OrderId = orderId;
            Status = status;
        }
    }
}
