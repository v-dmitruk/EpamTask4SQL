using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;
using System.Data.Common;
using System.Data.SqlTypes;

namespace EpamTask4SQL
{
    class ConnectionAdapter
    {
        string connectionString = System.Configuration.ConfigurationManager.ConnectionStrings["MyConnectionString"].ConnectionString;
        ProjectTypedDataSet typedDataSet = new ProjectTypedDataSet();

        public void Create(Employee item)
        {
            string query = "INSERT INTO Employee (Name, Surname, BirthDay) VALUES (@Name, @Surname, @BirthDay)";

            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                if (Get(item) == null)
                {
                    SqlCommand command = new SqlCommand(query, connection);


                    command.Connection.Open();
                    command.Parameters.AddWithValue("@Name", item.Name);
                    command.Parameters.AddWithValue("@Surname", item.Surname);
                    command.Parameters.AddWithValue("@BirthDay", item.BirthDay);

                    command.ExecuteNonQuery();
                }
                else
                {
                    Console.WriteLine($"There is already the same object in db with ID - {Get(item).ID}");
                }

            }
        }



        public Employee Get(Employee item)
        {
            string query = "SELECT * FROM Employee WHERE Name = @Name AND Surname = @Surname AND BirthDay = @BirthDay";
            Employee emp = null;
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                SqlCommand command = new SqlCommand(query, connection);

                command.Connection.Open();

                command.Parameters.AddWithValue("@Name", item.Name);
                command.Parameters.AddWithValue("@Surname", item.Surname);
                command.Parameters.AddWithValue("@BirthDay", item.BirthDay);

                SqlDataReader reader = command.ExecuteReader();

                while (reader.Read())
                {
                    emp = new Employee
                    {
                        ID = reader.GetInt32(0),
                        Name = reader.GetString(1),
                        Surname = reader.GetString(2),
                        BirthDay = reader.GetDateTime(3)
                    };
                }
            }
            return emp;
        }

        public IEnumerable<Employee> GetAll()
        {
            string query = "SELECT * FROM Employee";
            ICollection<Employee> employees = new List<Employee>();
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                SqlCommand command = new SqlCommand(query, connection);

                command.Connection.Open();
                SqlDataReader reader = command.ExecuteReader();
                while (reader.Read())
                {
                    employees.Add(new Employee
                    {
                        ID = reader.GetInt32(0),
                        Name = reader.GetString(1),
                        Surname = reader.GetString(2),
                        BirthDay = reader.GetDateTime(3)
                    });
                }
            }
            return employees;
        }

        public void Update(Employee item)
        {
            string query = $"UPDATE Employee SET Name = @Name, Surname = @Surname, BirthDay = @BirthDay WHERE ID = @ID";

            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                SqlCommand command = new SqlCommand(query, connection);

                command.Connection.Open();
                command.Parameters.AddWithValue("@ID", item.ID);
                command.Parameters.AddWithValue("@Name", item.Name);
                command.Parameters.AddWithValue("@Surname", item.Surname);
                command.Parameters.AddWithValue("@BirthDay", item.BirthDay);

                command.ExecuteNonQuery();
            }
        }


        public void Delete(Employee item)
        {
            string query = $"DELETE FROM Employee WHERE ID = @ID OR Name = @Name AND Surname = @Surname AND BirthDay = @BirthDay";

            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                SqlCommand command = new SqlCommand(query, connection);

                command.Connection.Open();
                command.Parameters.AddWithValue("@ID", item.ID);
                command.Parameters.AddWithValue("@Name", item.Name);
                command.Parameters.AddWithValue("@Surname", item.Surname);
                command.Parameters.AddWithValue("@BirthDay", item.BirthDay);
                try
                {
                    command.ExecuteNonQuery();
                }
                catch (SqlException ex)
                {
                    Exception error = new Exception("Cant delete employee", ex);
                    throw error;
                }
            }
        }

        public IEnumerable<Employee> Find(Func<Employee, bool> predicate)
        {
            return GetAll().Where(predicate).ToList();
        }

        public struct PostQuantity
        {
            public string postName;
            public int count;
        }

        public List<PostQuantity> GetPostCount()
        {
            //получить спиоск всех должностей с колличеством сотрудников на каждой из них
            string query = "SELECT EmployerPost, COUNT(EmployerID) FROM Tasks GROUP BY EmployerPost";
            List<PostQuantity> result = new List<PostQuantity>();
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                SqlCommand command = new SqlCommand(query, connection);

                command.Connection.Open();
                SqlDataReader reader = command.ExecuteReader();
                while (reader.Read())
                {
                    result.Add(new PostQuantity
                    {
                        postName = reader.GetString(0),
                        count = reader.GetInt32(1)
                    });
                }
            }
            return result;
        }

        public List<string> GetPostsWithoutEmployees()
        {
            //список должностей компании на которых нету сотрудников
            string query = "SELECT EmployerPost FROM Tasks WHERE EmployerID IS NULL GROUP BY EmployerPost";
            List<string> result = new List<string>();
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                SqlCommand command = new SqlCommand(query, connection);

                command.Connection.Open();
                SqlDataReader reader = command.ExecuteReader();
                while (reader.Read())
                {
                    result.Add(reader.GetString(0));
                }
            }
            return result;
        }

        public struct ProjectsWiThEmloyees
        {
            public string projectName;
            public int EmployeCount;
            public string PostName;
        }

        public List<ProjectsWiThEmloyees> GetProjectsWithEmployeesCount()
        {
            //получить спиоск проектов с колличеством сотрудников на каждой должности
            string query = "SELECT p.Name, t.EmployerPost, COUNT(t.EmployerID ) FROM Projects p INNER JOIN Tasks t ON p.ID = t.ProjectID GROUP BY p.Name, t.EmployerPost ORDER BY p.Name ASC";
            List<ProjectsWiThEmloyees> result = new List<ProjectsWiThEmloyees>();
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                SqlCommand command = new SqlCommand(query, connection);

                command.Connection.Open();
                SqlDataReader reader = command.ExecuteReader();
                while (reader.Read())
                {
                    result.Add(new ProjectsWiThEmloyees
                    {
                        projectName = reader.GetString(0),
                        EmployeCount = reader.GetInt32(2),
                        PostName = reader.GetString(1)
                    });
                }
            }
            return result;
        }

        public struct ProjectsWiThAverageTAsks
        {
            public string projectName;
            public int AverageTasksOnEachEmployee;
        }

        public List<ProjectsWiThAverageTAsks> GetAvarageAmountofEmloyeesTasksonEachproject()
        {
            //получить среднее колличество заданий на сотрудника для каждого проекта

            string query = "SELECT Name, AVG(result.count) FROM (SELECT ProjectID, EmployerID, COUNT(ID) AS count FROM Tasks GROUP BY ProjectID, EmployerID) AS result JOIN Projects ON ID=result.ProjectID GROUP BY ID, Name";
            List<ProjectsWiThAverageTAsks> result = new List<ProjectsWiThAverageTAsks>();
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                SqlCommand command = new SqlCommand(query, connection);

                command.Connection.Open();
                SqlDataReader reader = command.ExecuteReader();
                while (reader.Read())
                {
                    result.Add(new ProjectsWiThAverageTAsks
                    {
                        projectName = reader.GetString(0),
                        AverageTasksOnEachEmployee = reader.GetInt32(1)
                    });
                }
            }
            return result;
        }


        public void GetProjectLifetime()
        {
            //получить длительность выполнения кждого проекта

            string query = "SELECT Name, DATEDIFF(day, CreationDate, FinishDate) FROM Projects";
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                SqlCommand command = new SqlCommand(query, connection);

                command.Connection.Open();
                SqlDataReader reader = command.ExecuteReader();
                while (reader.Read())
                {
                    Console.WriteLine(reader.GetString(0) + " " + reader.GetInt32(1));
                }
            }
        }

        public void GetMinCountOfUnfinishedTasks()
        {
            //получить перечень сотрудников с минимальным колличеством незакрытых задач

            string query = "SELECT Name, result.count FROM (SELECT EmployerID, COUNT(ID) AS count FROM Tasks WHERE NOT TaskState = 'Finished' GROUP BY EmployerID HAVING COUNT(ID) = (SELECT MIN(count) AS min FROM (SELECT COUNT(ID) AS count FROM Tasks WHERE NOT TaskState = 'Finished' GROUP BY EmployerID) AS tab)) AS result JOIN Employee ON ID=result.EmployerID";
            //string query = "SELECT EmployerID, COUNT(ID) AS count FROM Tasks WHERE NOT TaskState = 'Finished' GROUP BY EmployerID";
            //string query = "SELECT * 
            //FROM 
            //    (SELECT Name, result.count 
            //    FROM 
            //    (SELECT EmployerID, COUNT(ID) AS count 
            //    FROM Tasks 
            //    WHERE NOT TaskState = 'Finished' 
            //    GROUP BY EmployerID) AS result 
            //    JOIN Employee ON ID=result.EmployerID) 
            //WHERE 
            //    count = 
            //    (SELECT MIN(COUNT(ID)) 
            //    FROM Tasks 
            //    WHERE NOT TaskState = 'Finished' 
            //    GROUP BY EmployerID)";
            //string query = "SELECT EmployerID, COUNT(ID) AS count FROM Tasks WHERE NOT TaskState = 'Finished' GROUP BY EmployerID HAVING COUNT(ID) = (SELECT MIN(count) AS min FROM (SELECT COUNT(ID) AS count FROM Tasks WHERE NOT TaskState = 'Finished' GROUP BY EmployerID) AS tab)";
            //string query = "SELECT MIN(count) AS min FROM (SELECT COUNT(ID) AS count FROM Tasks WHERE NOT TaskState = 'Finished' GROUP BY EmployerID) AS tab";

            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                SqlCommand command = new SqlCommand(query, connection);

                command.Connection.Open();
                SqlDataReader reader = command.ExecuteReader();
                while (reader.Read())
                {
                    Console.WriteLine(reader.GetString(0) + " " + reader.GetInt32(1));
                }
            }
        }


        public void GetMaxCountOfUnfinishedTasksOverDeadline()
        {
            //получить перечень сотрудников с максимальным колличеством незакрытых задач по которых уже истёк дедлайн

            string query = "SELECT Name, result.count FROM (SELECT EmployerID, COUNT(ID) AS count FROM Tasks WHERE Deadline < SYSDATETIME() AND NOT TaskState = 'Finished' GROUP BY EmployerID HAVING COUNT(ID) = (SELECT MAX(count) AS min FROM (SELECT COUNT(ID) AS count FROM Tasks WHERE NOT TaskState = 'Finished' GROUP BY EmployerID) AS tab)) AS result JOIN Employee ON ID=result.EmployerID";

            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                SqlCommand command = new SqlCommand(query, connection);

                command.Connection.Open();
                SqlDataReader reader = command.ExecuteReader();
                while (reader.Read())
                {
                    Console.WriteLine(reader.GetString(0) + " " + reader.GetInt32(1));
                }
            }
        }

        public void AddFiveDaysToUnfinishedTasksDeadline(int ProjectID)
        {
            //продлить дедлайн незакрытых задач на 5 дней

            string query = $"UPDATE Tasks SET Deadline = DATEADD(dd, 5, Deadline) WHERE ProjectID = @ID AND NOT TaskState = 'Finished'";

            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                SqlCommand command = new SqlCommand(query, connection);

                command.Connection.Open();
                command.Parameters.AddWithValue("@ID", ProjectID);
                command.ExecuteNonQuery();

            }
        }

        public void GetUnstartedTasksCountForEachProject()
        {
            //посчитать на каждом проекте количесвто задач к которым ещё не приступили        

            string query = "SELECT pr.Name, COUNT(Tasks.ID) as count FROM Tasks JOIN Projects AS pr ON pr.ID = Tasks.ProjectID WHERE EmployerID IS NULL AND Tasks.TaskState = 'Opened' GROUP BY Name";

            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                SqlCommand command = new SqlCommand(query, connection);

                command.Connection.Open();
                SqlDataReader reader = command.ExecuteReader();
                while (reader.Read())
                {
                    Console.WriteLine(reader.GetString(0) + " " + reader.GetInt32(1));
                }
            }
        }


        public void SetProjectsAsFinishedWhenAllTasksAreFinished()
        {
            //перевести проекты в состояние закрыт, для которых все задачи закрыты и задать время закрытия временем закрытия задачи проекта, принятой последней


            //string query = "UPDATE pr SET pr.FinishDate = ts.lastTask, pr.Finished = 'True' FROM Projects AS pr WHERE BLABLA JOIN (SELECT ProjectID, MAX(StateChangedDate) AS lastTask FROM Tasks WHERE TaskStatus = 'Finished' GROUP BY ProjectID) AS ts ON ts.ProjectID = pr.ID";
            //string query = "MERGE INTO Projects pr " +
            //    "USING (SELECT ProjectID, MAX(StateChangedDate) AS last FROM Tasks WHERE NOT TaskState = 'Finished' GROUP BY ProjectID) " +
            //    "AS real " +
            //    "ON pr.iD = real.ProjectID " +
            //    "WHEN MATCHED THEN " +
            //    "UPDATE " +
            //    "SET pr.FinishDate = real.last, pr.Finished = 'True';";
            string query = "UPDATE pr " +
                "SET pr.FinishDate = lt.last, pr.Finished = 'True' " +
                "FROM Projects as pr " +
                    "JOIN " +
                        "(SELECT ts.ProjectID AS ID, MAX(ts.StateChangedDate) AS last FROM Tasks AS ts JOIN (SELECT ID FROM Projects AS prs LEFT JOIN (SELECT ProjectID FROM Tasks WHERE NOT TaskState = 'Finished' GROUP BY ProjectID) AS bad ON bad.ProjectID = prs.ID WHERE bad.ProjectID IS NULL) AS good ON ts.ProjectID = good.ID GROUP BY ts.ProjectID) " +
                    "AS lt " +
                    "ON pr.ID = lt.ID";
//                "WHERE pr.ID = lt.ID";


//    "FROM Projects AS prs " +
//       "LEFT JOIN " +
//            "(SELECT ProjectID AS lastTask FROM Tasks WHERE NOT TaskStatus = 'Finished' GROUP BY ProjectID) AS ts " +
//            "ON ts.ProjectID = prs.ID " +
//        "WHERE ts.ProjectID IS NULL " +




            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                SqlCommand command = new SqlCommand(query, connection);

                command.Connection.Open();
                command.ExecuteNonQuery();
            }
        }

        public void GetProjectsAndEmployersWithAllFinishedTasks()
        {
            //выяснить по всем проектам, какие сотрудники на проекте не имеют незакрытых задач
            string query = "SELECT pr.Name, em.Name FROM (SELECT ts.ProjectID, ts.EmployerID FROM (SELECT ProjectID, EmployerID FROM Tasks WHERE NOT EmployerID IS NULL GROUP BY ProjectID, EmployerID) AS ts LEFT JOIN (SELECT ProjectID, EmployerID FROM Tasks WHERE NOT TaskState = 'Finished' GROUP BY ProjectID, EmployerID) AS bad ON ts.ProjectID = bad.ProjectID AND ts.EmployerID = bad.EmployerID WHERE bad.ProjectID IS NULL OR bad.EmployerID IS NULL) AS good " +
                "JOIN Projects AS pr ON pr.ID = good.ProjectID " +
                "JOIN Employee AS em ON em.ID = good.EmployerID";

            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                SqlCommand command = new SqlCommand(query, connection);

                command.Connection.Open();
                SqlDataReader reader = command.ExecuteReader();
                while (reader.Read())
                {
                    Console.WriteLine(reader.GetString(0) + " " + reader.GetString(1));
                }
            }
        }

        public void MoveLazyEmloyerToTask(string name)
        {
            //Заданную задачу (по названию) проекта перевести на сотрудника с минимальным количеством выполняемых им задач            
            //string query = $"UPDATE ts SET ts.EmployerID = KQKQ FROM Tasks AS ts JOIN () AS fin ON ts.ID = fin.ID";
            //string query = $"UPDATE ts " +
            //    $"SET ts.EmployerID = (SELECT TOP 1 EmployerID FROM Tasks GROUP BY EmployerID HAVING COUNT(ID)=(SELECT MIN(COUNT(ID)) FROM Tasks WHERE NOT EmployerID IS NULL GROUP BY EmployerID) AS qw) AS qq " +
            //    $"FROM Tasks AS ts " +
            //    $"WHERE ts.TaskName=@Name";

            //string query = $"UPDATE ts " +
            //    $"SET ts.EmployerID = " +
            //        $"(SELECT TOP 1 tsk.EmployerID " +
            //        $"FROM Tasks AS tsk " +
            //        $"WHERE COUNT(tsk.ID) = " +
            //            $"(SELECT MIN(COUNT(tsks.ID)) " +
            //            $"FROM Tasks AS tsks " +
            //            $"WHERE NOT EmployerID IS NULL " +
            //            $"GROUP BY EmployerID) " +
            //        $"GROUP BY EmployerID) " +
            //    $"FROM Tasks AS ts " +
            //    $"WHERE ts.TaskName=@Name";


            //string query = $"UPDATE ts " +
            //$"SET ts.EmployerID = s.ID " +
            //$"FROM Tasks AS ts JOIN (SELECT ID FROM Employee JOIN (SELECT TOP 1 EmployerID FROM Tasks WHERE COUNT(ID) = '10' GROUP BY EmployerID) AS q ON Employee.ID = q.EmployerID) AS s ON ts.EmployerID = s.min " +
            //$"WHERE ts.TaskName=@Name";


            ////РАБОЧИЙ НО НЕМНОГО НЕ ТАК
            //string query = $"UPDATE ts " +
            //$"SET ts.EmployerID = (SELECT TOP 1 ID FROM Employee JOIN (SELECT EmployerID FROM Tasks WHERE NOT TaskState = 'Finished' GROUP BY EmployerID HAVING COUNT(ID) = (SELECT MIN(count) AS min FROM (SELECT COUNT(ID) AS count FROM Tasks WHERE NOT TaskState = 'Finished' GROUP BY EmployerID) AS tab)) AS b ON ID = b.EmployerID) " +
            //$"FROM Tasks AS ts " +
            //$"WHERE ts.TaskName=@Name";

            string query = $"UPDATE ts " +
            $"SET ts.EmployerID = (SELECT TOP 1 ID FROM Employee AS a LEFT JOIN (SELECT EmployerID AS eid, COUNT(ID) AS tsks FROM Tasks WHERE NOT TaskState = 'Finished' GROUP BY EmployerID HAVING NOT EmployerID IS NULL) AS b ON a.ID=b.eid where ISNULL(b.tsks, 0) = (SELECT MIN(ISNULL(b.tsks, 0)) FROM Employee AS a LEFT JOIN (SELECT EmployerID AS eid, COUNT(ID) AS tsks FROM Tasks WHERE NOT TaskState = 'Finished' GROUP BY EmployerID HAVING NOT EmployerID IS NULL) AS b ON a.ID=b.eid)) " +
            $"FROM Tasks AS ts " +
            $"WHERE ts.TaskName=@Name";

            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                SqlCommand command = new SqlCommand(query, connection);

                command.Connection.Open();
                command.Parameters.AddWithValue("@Name", name);
                command.ExecuteNonQuery();
            }
        }
    }
}
