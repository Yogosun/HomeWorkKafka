namespace HomeworkAA.Entity
{
    public abstract class MenuItem
    {
        public int Id { get; }
        public string Name { get; }
        public decimal Price { get; }

        protected MenuItem(int id, string name, decimal price)
        {
            Id = id;
            Name = name;
            Price = price;
        }

        public override string ToString() => $"{Name} - {Price} руб.";
    }
}
