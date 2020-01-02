using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EpamTask4SQL
{
    class Task
    {
        public int ID;
        public string Name;
        public string Description;
        public int ProjectID;
        public int EmployerID;
        public string EmployerPost;
        public int ChiefID;
        public enum State
        {
            Opened,
            Closed, 
            Paused,
            Finished
        }
        public DateTime StateChangedDate;
        public DateTime Deadline;
    }
}
