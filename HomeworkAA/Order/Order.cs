using HomeworkAA.Baskets;
using HomeworkAA.Entity;

namespace HomeworkAA.Order
{
    public class Order
    {
        public int OrderId { get; }
        public OrderStatus Status { get; private set; }
        public Basket UserBasket { get; }

        public Order(int orderId, Basket basket)
        {
            OrderId = orderId;
            UserBasket = basket;
            Status = OrderStatus.Created;
        }

        public void ChangeStatus(OrderStatus newStatus)
        {
            Status = newStatus;
        }
    }
}
