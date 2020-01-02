using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


namespace EpamTask4SQL
{
    class Program
    {
        static void Main(string[] args)
        {
            Employee lol1 = new Employee() { BirthDay = new DateTime(1991, 12, 31), Name = "Vasya" , Surname = "Cherchill" };
            Employee lol2 = new Employee() { BirthDay = new DateTime(1991, 12, 31), Name = "Lesya", Surname = "Cherchill" };
            Employee lol3 = new Employee() { BirthDay = new DateTime(1991, 12, 31), Name = "Richie", Surname = "Cherchill" };
            Employee lol4 = new Employee() { BirthDay = new DateTime(1991, 12, 31), Name = "Kourilin", Surname = "Cherchill" };
            ConnectionAdapter DB = new ConnectionAdapter();
            Console.WriteLine("Adding the employers");
            try
            {
                DB.Create(lol1);
                DB.Create(lol2);
                DB.Create(lol3);
                DB.Create(lol4);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
            DB.Delete(lol3);
            Console.WriteLine("Listing the Employers");
            List<Employee> list = (List<Employee>)DB.GetAll();
            foreach (Employee item in list)
            {
                Console.WriteLine($"ID - {item.ID}, Name - {item.Name}, Birthday - {item.BirthDay.ToString("D")}");
            }
            Console.WriteLine("================ Starting the queries");
            //получить спиоск всех должностей с колличеством сотрудников на каждой из них
            var list1 = DB.GetPostCount();
            foreach (ConnectionAdapter.PostQuantity item in list1)
            {
                Console.WriteLine($"Post - {item.postName} has {item.count} employers");
            }
            Console.WriteLine("================");
            var list2 = DB.GetPostsWithoutEmployees();
            foreach (string item in list2)
            {
                Console.WriteLine($"Post - {item} has 0 employers");
            }
            Console.WriteLine("================");
            var list3 = DB.GetProjectsWithEmployeesCount();
            foreach (ConnectionAdapter.ProjectsWiThEmloyees item in list3)
            {
                Console.WriteLine($"Project - {item.projectName} has {item.EmployeCount} employers on {item.PostName} post");
            }
            Console.WriteLine("================");

            var list4 = DB.GetAvarageAmountofEmloyeesTasksonEachproject();
            foreach (ConnectionAdapter.ProjectsWiThAverageTAsks item in list4)
            {
                Console.WriteLine($"Project - {item.projectName} has {item.AverageTasksOnEachEmployee} tasks on each employer");
            }
            Console.WriteLine("================");
            DB.GetProjectLifetime();
            Console.WriteLine("================");
            DB.GetMinCountOfUnfinishedTasks();
            Console.WriteLine("================");
            DB.GetMaxCountOfUnfinishedTasksOverDeadline();
            Console.WriteLine("================");
            DB.AddFiveDaysToUnfinishedTasksDeadline(3);
            Console.WriteLine("5 days added, check Database");
            Console.WriteLine("================");
            DB.GetUnstartedTasksCountForEachProject();
            Console.WriteLine("================");
            DB.SetProjectsAsFinishedWhenAllTasksAreFinished();
            Console.WriteLine("Maybe some Projects were set to finished and their dates were set to last finished task on these projects");
            Console.WriteLine("================");
            DB.GetProjectsAndEmployersWithAllFinishedTasks();
            Console.WriteLine("================");
            DB.MoveLazyEmloyerToTask("TaskToMove");
            Console.WriteLine("Employee was assigned to named task");
            Console.WriteLine("================");

            Console.ReadLine();

        }



    }
}
