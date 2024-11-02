using HomeworkAA.Entity;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HomeworkAA.Menus.Food
{
    public class Salad : MenuItem
    {
        public Salad(int id) : base(id, "Салат", 150m) { }
    }
}
