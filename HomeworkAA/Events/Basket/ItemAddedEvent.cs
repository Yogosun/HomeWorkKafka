namespace HomeworkAA.Events.Basket
{
    public class ItemAddedEvent : BasketEvent
    {
        public string Item { get; }
        public decimal Price { get; }

        public ItemAddedEvent(string item, decimal price)
        {
            Item = item;
            Price = price;
        }
    }
}
