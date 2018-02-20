using System;
using System.Collections.Generic;
using System.Linq;
using System.IO;
using Newtonsoft.Json.Linq;
using unirest_net.http;
using System.Data.SqlClient;
using System.Data;

namespace LogBaseCalls {
    class Program {
        public static Dictionary<int, string> ownersData = new Dictionary<int, string>();
        public static Dictionary<int, string> outcomeData = new Dictionary<int, string>();
        public static List<Owner> owners;
        public static List<Call> conList;
        public static StreamWriter log;
        public static string[] resTypes = { "lead", "contact" };
        public static string[] ocTypes = { "leadToCon", "other" };
        public static int[] conToLeadOutcomes = { 1283017, 1341185, 1350524, 1394566 };
        public static string token = "";
        public static Random random = new Random();
        public static string connString = "Data Source=RALIMSQL1;Initial Catalog=CAMSRALFG;Integrated Security=SSPI;";
        public static DateTime limit;
        public static string line = @"INSERT INTO [CAMSRALFG].[dbo].[BaseCalls] ([id],[user_id],[name], [outcome_id],"+
            "[outcome],[duration],[phone_number],[incoming],[missed],[updated_at],[made_at],[resource_id],[resource_type]) "+
            "VALUES (@id,@user_id,@name,@outcome_id,@outcome,@duration,@phone_number,@incoming,@missed,@updated_at,@made_at,@resource_id,@resource_type);";

        static void Main(string[] args) {
            string startURL = @"https://api.getbase.com/v2/calls?per_page=100";
            DateTime now = DateTime.Now;
            string logPath = @"\\NAS3\NOE_Docs$\RALIM\Logs\Base\CallCheck_" + now.ToString("ddMMyyyy") + ".txt";
            if (!File.Exists(logPath)) {
                using (StreamWriter sw = File.CreateText(logPath)) {
                    sw.WriteLine("Creating call check log file for " + now.ToString("ddMMyyyy") + " at " + now.ToString());
                }
            }

            log = File.AppendText(logPath);
            log.WriteLine("\n\nStarting call check at " + now);
            Console.WriteLine("Starting call check at " + now);

            limit = GetLastDate();
            owners = new List<Owner>();
            conList = new List<Call>();
            var fs = new FileStream(@"C:\apps\NiceOffice\token", FileMode.Open, FileAccess.Read, FileShare.ReadWrite);

            using (var sr = new StreamReader(fs)) {
                token = sr.ReadToEnd();
            }

            string hash = RandomString(10);
            string me = Environment.UserDomainName.ToString() + @"\" + Environment.UserName;

            SetOwnerData();
            SetOutcomeData();

            string testJSON = Get(startURL, token);
            //DateTime lastWeek = DateTime.Now.Date.AddDays(-7);

            Console.WriteLine(limit + " to " + now);

            string nextURL = startURL;
            var jArr = JArray.Parse(@"[{'data':{'made_at':'" + DateTime.UtcNow + @"'}}]"); // will always be greater
            var jsonObj = new JObject();

            while (Convert.ToDateTime(jArr.Last["data"]["made_at"]).ToLocalTime() > limit) {
                Console.WriteLine(Convert.ToDateTime(jArr.Last["data"]["made_at"]).ToLocalTime() + " > " + limit);
                log.WriteLine(Convert.ToDateTime(jArr.Last["data"]["made_at"]).ToLocalTime() + " > " + limit);

                jsonObj = JObject.Parse(Get(nextURL, token)) as JObject;
                nextURL = jsonObj["meta"]["links"]["next_page"].ToString();
                jArr = jsonObj["items"] as JArray;

                foreach (var v in jArr) {
                    var data = v["data"];
                    int user_id = Convert.ToInt32(data["user_id"]);
                    if (!ownersData.ContainsKey(user_id)) {
                        continue;
                    }

                    bool missed = true;
                    if (data["missed"] == null && data["duration"] != null &&
                        Convert.ToInt32(data["duration"]) > 0) {
                        missed = false;
                    }
                    else if (data["missed"] != null && data["missed"].ToString() != "") {
                        missed = Convert.ToBoolean(data["missed"]);
                    }

                    if (missed) {
                        continue;
                    }

                    int id = Convert.ToInt32(data["id"]);
                    DateTime made_at = Convert.ToDateTime(data["made_at"]).ToLocalTime();
                    DateTime updated_at = Convert.ToDateTime(data["updated_at"]).ToLocalTime();
                    string name = ownersData[user_id];


                    string resource_type = "unknown";
                    if (data["resource_type"] != null && data["resource_type"].ToString() != "") {
                        resource_type = data["resource_type"].ToString();
                    }


                    int outcome_id = 0;

                    if (data["outcome_id"] != null && data["outcome_id"].ToString() != "") {
                        outcome_id = Convert.ToInt32(data["outcome_id"]);
                    }

                    string outcome = "Unknown";
                    if (outcomeData.ContainsKey(outcome_id)) {
                        outcome = outcomeData[outcome_id];
                    }
                    string outcome_type = conToLeadOutcomes.Contains(outcome_id) ? "leadToCon" : "other";

                    int duration = 0;
                    if (data["duration"] != null && data["duration"].ToString() != "") {
                        duration = Convert.ToInt32(data["duration"]);
                    }
                    bool incoming = Convert.ToBoolean(data["incoming"]);
                    string phone_number = data["phone_number"].ToString();

                    int resource_id = 0;
                    if(data["resource_id"] != null && data["resource_id"].ToString() != ""){
                        resource_id = Convert.ToInt32(data["resource_id"]);
                    }


                    if (made_at > limit) {
                        Call tCall = new Call();
                        tCall.id = id;
                        tCall.user_id = user_id;
                        tCall.name = name;
                        //tCall.summary = summary;
                        //tCall.recording_url = recording_url;
                        tCall.outcome_id = outcome_id;
                        tCall.outcome = outcome ;
                        tCall.duration = duration;
                        tCall.phone_number = phone_number;
                        tCall.incoming = incoming;
                        tCall.missed = missed;
                        tCall.updated_at = updated_at;
                        tCall.made_at = made_at;
                        //tCall.external_id = external_id;
                        //tCall.associated_deal_ids = associated_deal_ids;
                        tCall.resource_id = resource_id;
                        tCall.resource_type = resource_type;
                        conList.Add(tCall);
                    }
                    else {
                        Console.WriteLine(made_at + " is too old");
                        log.WriteLine(made_at + " is too old");
                        break;
                    }
                }
            }

            //StreamWriter file = new StreamWriter("H:\\Desktop\\CallDataLine.csv");
            log.WriteLine("id,user_id,name,outcome_id,outcome,duration,phone_number,incoming,missed,updated_at,resource_id,resource_type");
            foreach (Call tCall in conList) {
                log.WriteLine(tCall.id + ", " + tCall.user_id + ", " + tCall.name + ", " + tCall.outcome_id + ", " + tCall.outcome + 
                    ", " + tCall.duration + ", " + tCall.phone_number + ", " + tCall.incoming + ", " + tCall.missed + ", " + 
                    tCall.updated_at + ", " + tCall.resource_id + ", " + tCall.resource_type);
            }
            log.Flush();
            //file.Flush();
            //file.Close();

            using (SqlConnection connection = new SqlConnection(connString)) {

                foreach (Call call in conList) {
                    using (SqlCommand command = new SqlCommand(line, connection)) {
                        command.Parameters.Add("@id", SqlDbType.Int).Value = call.id;
                        command.Parameters.Add("@user_id", SqlDbType.Int).Value = call.user_id;
                        command.Parameters.Add("@name", SqlDbType.NVarChar).Value = call.name;
                        command.Parameters.Add("@outcome_id", SqlDbType.Int).Value = call.outcome_id;
                        command.Parameters.Add("@outcome", SqlDbType.NVarChar).Value = call.outcome;
                        command.Parameters.Add("@duration", SqlDbType.Int).Value = call.duration;
                        command.Parameters.Add("@phone_number", SqlDbType.NVarChar).Value = call.phone_number;
                        command.Parameters.Add("@incoming", SqlDbType.Bit).Value = call.incoming;
                        command.Parameters.Add("@missed", SqlDbType.Bit).Value = call.missed;
                        command.Parameters.Add("@updated_at", SqlDbType.DateTime).Value = call.updated_at;
                        command.Parameters.Add("@made_at", SqlDbType.DateTime).Value = call.made_at;
                        command.Parameters.Add("@resource_id", SqlDbType.Int).Value = call.resource_id;
                        command.Parameters.Add("@resource_type", SqlDbType.NVarChar).Value = call.resource_type;

                        try {
                            connection.Open();

                            int result = command.ExecuteNonQuery();

                            if (result < 0) {
                                log.WriteLine("INSERT failed for " + command.ToString());
                                log.Flush();
                                Console.WriteLine("INSERT failed for " + command.ToString());
                            }
                        }
                        catch (Exception ex) {
                            log.WriteLine(ex);
                            log.Flush();
                            Console.WriteLine(ex);
                        }
                        finally {
                            connection.Close();
                        }
                    }
                }
            }
            log.WriteLine("Done at " + DateTime.Now);
            Console.WriteLine("Done!");
        }

        public static string Get(string url, string token) {
            string body = "";
            try {
                HttpResponse<string> jsonReponse = Unirest.get(url)
                    .header("accept", "application/json")
                    .header("Authorization", "Bearer " + token)
                    .asJson<string>();
                body = jsonReponse.Body.ToString();
                return body;
            }
            catch (Exception ex) {
                log.WriteLine(ex);
                log.Flush();
                Console.WriteLine(ex);
                return body;
            }
        }

        public static DateTime GetLastMonday(DateTime dt) {
            if (dt.DayOfWeek == DayOfWeek.Monday) {
                return dt.AddDays(-7).Date;
            }
            bool stop = false;
            DateTime temp = dt.AddDays(-1);
            while (!stop) {
                if (temp.DayOfWeek == DayOfWeek.Monday) {
                    stop = true;
                }
                else {
                    temp = temp.AddDays(-1);
                }
            }
            return temp.Date;
        }

        public static DateTime GetLastDate() {
            DateTime limit = new DateTime();
            using (SqlConnection connection = new SqlConnection(connString)) {
                string sqlStr = "SELECT MAX([made_at]) FROM [CAMSRALFG].[dbo].[BaseCalls];";
                using (SqlCommand command = new SqlCommand(sqlStr, connection)) {

                    try {
                        connection.Open();

                        SqlDataReader result = command.ExecuteReader();

                        while (result.Read()) {
                            if (!result.IsDBNull(0)) {
                                limit = result.GetDateTime(0);
                            }
                        }

                        if (limit == DateTime.MinValue) {
                            return DateTime.Now.Date.AddDays(-7);
                        }
                        else log.WriteLine("Found max date of " + limit);
                    }
                    catch (Exception ex) {
                        log.WriteLine(ex);
                        log.Flush();
                        Console.WriteLine(ex);
                    }
                    finally {
                        connection.Close();
                    }
                }

            }
            return limit;
        }


        public static void SetOwnerData() {
            string testJSON = Get(@"https://api.getbase.com/v2/users?per_page=100&sort_by=created_at&status=active", token);
            JObject jObj = JObject.Parse(testJSON) as JObject;
            JArray jArr = jObj["items"] as JArray;
            foreach (var obj in jArr) {
                var data = obj["data"];

                if (data["group"].HasValues == false || Convert.ToInt32(data["group"]["id"]) != 84227) {
                    continue; //do not count agents not in sales group stats.
                }

                int tID = Convert.ToInt32(data["id"]);
                string tName = data["name"].ToString();
                ownersData.Add(tID, tName);
            }
        }

        public static void SetOutcomeData() {
            string testJSON = Get(@"https://api.getbase.com/v2/call_outcomes?per_page=100", token);
            JObject jObj = JObject.Parse(testJSON) as JObject;
            JArray jArr = jObj["items"] as JArray;
            outcomeData.Add(0, "Unknown");

            foreach (var obj in jArr) {
                var data = obj["data"];
                int tID = Convert.ToInt32(data["id"]);
                string tName = data["name"].ToString();
                outcomeData.Add(tID, tName);
            }
        }

        public static string RandomString(int length) {
            const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
            return new string(Enumerable.Repeat(chars, length)
              .Select(s => s[random.Next(s.Length)]).ToArray());
        }
    }
}
