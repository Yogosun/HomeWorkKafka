using HomeworkAA.Entity;
using HomeworkAA.Menus.Food;
using System.Collections.Generic;

namespace HomeworkAA.Menus
{
    public class Menu
    {
        public List<MenuItem> Items { get; }

        public Menu()
        {
            Items = new List<MenuItem>
            {
                new Pizza(1),
                new Burger(2),
                new Sushi(3),
                new Pasta(4),
                new Salad(5)
            };
        }

        public MenuItem GetMenuItemById(int id)
        {
            return Items.Find(item => item.Id == id);
        }
    }
}
