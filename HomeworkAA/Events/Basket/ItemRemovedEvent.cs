namespace HomeworkAA.Events.Basket
{
    public class ItemRemovedEvent : BasketEvent
    {
        public string Item { get; }

        public ItemRemovedEvent(string item)
        {
            Item = item;
        }
    }
}
