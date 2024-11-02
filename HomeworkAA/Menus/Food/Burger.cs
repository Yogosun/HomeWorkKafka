using HomeworkAA.Entity;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HomeworkAA.Menus.Food
{
    public class Burger : MenuItem
    {
        public Burger(int id) : base(id, "Бургер", 250m) { }
    }
}
