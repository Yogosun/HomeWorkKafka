using HomeworkAA.Entity;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HomeworkAA.Menus.Food
{
    public class Pizza : MenuItem
    {
        public Pizza(int id) : base(id, "Пицца", 500m) { }
    }
}
