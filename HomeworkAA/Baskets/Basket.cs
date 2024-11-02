using System.Collections.Generic;
using System.Linq;

namespace HomeworkAA.Baskets
{
    public class Basket
    {
        private readonly List<(string Item, decimal Price)> _items = new List<(string, decimal)>();

        public void AddItem(string item, decimal price)
        {
            _items.Add((item, price));
        }

        public void RemoveItem(string item)
        {
            _items.RemoveAll(i => i.Item == item);
        }

        public decimal CalculateTotal()
        {
            return _items.Sum(i => i.Price);
        }

        public IReadOnlyList<(string Item, decimal Price)> GetItems() => _items.AsReadOnly();

        public void Clear()
        {
            _items.Clear();
        }
    }

}
