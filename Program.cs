/* Copyright 2021 Esri
 *
 * Licensed under the Apache License Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace HttpPostSender
{
    class Program
    {
        
        private static string receiverUrl = ConfigurationManager.AppSettings["receiverUrl"];
        private static bool authenticationArcGIS = Boolean.Parse(ConfigurationManager.AppSettings["authenticationArcGIS"]);
        private static string tokenPortalUrl = ConfigurationManager.AppSettings["tokenPortalUrl"];
        private static string username = ConfigurationManager.AppSettings["username"];
        private static string password = ConfigurationManager.AppSettings["password"];
        private static string fileUrl = ConfigurationManager.AppSettings["fileUrl"];
        private static bool hasHeaders = Boolean.Parse(ConfigurationManager.AppSettings["hasHeaders"]);
        private static string fieldDelimiter = ConfigurationManager.AppSettings["fieldDelimiter"];
        private static int numLinesPerBatch = Int32.Parse(ConfigurationManager.AppSettings["numLinesPerBatch"]);
        private static int sendInterval = Int32.Parse(ConfigurationManager.AppSettings["sendInterval"]);
        private static int timeField = Int32.Parse(ConfigurationManager.AppSettings["timeField"]);
        private static bool setToCurrentTime = Boolean.Parse(ConfigurationManager.AppSettings["setToCurrentTime"]);
        private static string dateFormat = ConfigurationManager.AppSettings["dateFormat"];
        private static CultureInfo dateCulture = CultureInfo.CreateSpecificCulture(ConfigurationManager.AppSettings["dateCulture"]);
        private static bool repeatSimulation = Boolean.Parse(ConfigurationManager.AppSettings["repeatSimulation"]);

        private static readonly HttpClient client = new HttpClient();
        
        static async Task Main()
        {
            //Console.WriteLine("Starting...");
            try
            {   
                Console.WriteLine($"Fetching and reading file: {fileUrl}");
                HttpWebRequest myHttpWebRequest = (HttpWebRequest)WebRequest.Create(fileUrl);
                // Sends the HttpWebRequest and waits for the response.			
                HttpWebResponse myHttpWebResponse = (HttpWebResponse)myHttpWebRequest.GetResponse();
                // Gets the stream associated with the response.
                Stream receiveStream = myHttpWebResponse.GetResponseStream();
                Encoding encode = System.Text.Encoding.GetEncoding("utf-8");
                // Pipes the stream to a higher level stream reader with the required encoding format. 
                StreamReader readStream = new StreamReader(receiveStream, encode);
                string line;
                string headerLine;
                string[] fields = null;
                string token = "";
                JObject schema =  new JObject();

                // Read lines from the file until the end of 
                // the file is reached.
                string[] contentArray = readStream.ReadToEnd().Replace("\r", "").Split('\n');

                readStream.Close();

                int c = contentArray.Length;
                bool runTask = true;


    
                if ((headerLine = contentArray[0]) != null)
                {
                    //schema = new JObject();
                    fields = headerLine.Split(fieldDelimiter);
                    int fieldNum = 1;
                    foreach (string fieldName in fields)
                    {
                        if (hasHeaders){
                            schema[fieldName] = null;
                        }
                        else{  
                            string genericFieldName = $"field{fieldNum}";                              
                            schema[genericFieldName] = null;
                        }
                        fieldNum += 1;
                    }
                    Console.WriteLine("Schema created based on the incoming data:");
                    Console.WriteLine(schema);
                    Dictionary<string,string> dictObj = schema.ToObject<Dictionary<string,string>>();
                    dictObj.Keys.CopyTo(fields,0);
                }

                client.DefaultRequestHeaders.TryAddWithoutValidation("Accept", "*/*");
                client.DefaultRequestHeaders.TryAddWithoutValidation("Referer", "http://localhost:8888");
                client.DefaultRequestHeaders.TryAddWithoutValidation("Content-Type", "application/json; charset=utf-8");
                if (authenticationArcGIS){
                    string tokenStr = await getToken(tokenPortalUrl,username,password,21600); 
                    dynamic tokenJson = JsonConvert.DeserializeObject(tokenStr); 
                    token = tokenJson["token"];                                    
                    client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token);
                }
                                            
                              
                
                int count = 0;
                int countTotal = 0;
                JArray eventBatch = null; 
                
                var stopwatch = new Stopwatch();
                while (runTask)
                {
                    for (int l = (hasHeaders ? 1 : 0); l < c; l++)
                    {
                        line = contentArray[l];

                        // Create a batch of events if needed
                        if (eventBatch == null) 
                        {
                            
                            eventBatch = new JArray();
                            stopwatch.Start();
                        }
                        eventBatch = eventBatch ?? new JArray(); 
                        dynamic[] values = line.Split(fieldDelimiter);
                        for (int i = 0; i < schema.Count; i++)
                        { 
                            long longVal = 0;
                            decimal decVal = 0;
                            bool isLong = long.TryParse(values[i], out longVal);
                            bool isDec = decimal.TryParse(values[i], out decVal);
                            schema[fields[i]] = isLong ? longVal : isDec ? decVal : values[i];
                        }
                        if (setToCurrentTime)
                        {
                            if (String.IsNullOrEmpty(dateFormat))
                            {
                                schema[fields[timeField]] = new DateTimeOffset(DateTime.Now).ToUnixTimeMilliseconds();
                            }
                            else
                            {
                                try{
                                    schema[fields[timeField]] = DateTime.Now.ToString(dateFormat,dateCulture);
                                }
                                catch(Exception e){
                                    schema[fields[timeField]] = new DateTimeOffset(DateTime.Now).ToUnixTimeMilliseconds();
                                }
                            }
                        }
                        
                        count++;

                        // Add events to the batch.  
                        eventBatch.Add(schema);

                        
                        if (count == numLinesPerBatch)
                        {

                           
                            // send the batch of events to the REST endpoint
                            string payload = JsonConvert.SerializeObject(eventBatch);
                            var content = new StringContent(payload, System.Text.Encoding.UTF8, "application/json");
                            content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
                            var response = await client.PostAsync(receiverUrl, content); 
                            var responseString = await response.Content.ReadAsStringAsync();
                            dynamic responseJson = JsonConvert.DeserializeObject(responseString);
                            //if the request failed because the token expired, get a new one and retry the request
                            if (!response.IsSuccessStatusCode && responseJson["error"]["code"] == 403 && authenticationArcGIS){                                    
                                Console.WriteLine($"Renewing the token for {username}");
                                string tokenStr = await getToken(tokenPortalUrl,username,password,21600); 
                                dynamic tokenJson = JsonConvert.DeserializeObject(tokenStr); 
                                token = tokenJson["token"];                                    
                                client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer",token);
                                response = await client.PostAsync(receiverUrl, content); 
                                responseString = await response.Content.ReadAsStringAsync();
                                responseJson = JsonConvert.DeserializeObject(responseString);                               
                            }
                            if (response.IsSuccessStatusCode){
                                countTotal += count;
                                eventBatch = null;
                                stopwatch.Stop();
                                int elapsed_time = (int)stopwatch.ElapsedMilliseconds;
                                stopwatch.Reset();
                                
                                if (elapsed_time < sendInterval) {
                                    Console.WriteLine(string.Format($"A batch of {count} events has been sent. It took {elapsed_time} milliseconds. Waiting for {sendInterval - elapsed_time} milliseconds. Total sent: {countTotal}."));
                                    Thread.Sleep(sendInterval - elapsed_time);
                                }
                                else
                                {
                                    Console.WriteLine(string.Format($"A batch of {count} events has been sent. It took {elapsed_time} milliseconds.  Total sent: {countTotal}."));
                                }
                                count = 0;
                            }
                            else{
                                return;
                            }

                        }
                    }
                    Console.WriteLine(string.Format($"Reached the end of the simulation file. Repeat is set to {repeatSimulation}"));
                    if (!repeatSimulation)
                    {
                        runTask = false;
                    }
                }
                
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                Console.WriteLine(e.StackTrace);
                Console.WriteLine(e.Data);
            }
        }

        static async Task<string> getToken(string url, string user, string pass, double expiry)
        {    
            try
            {        
                var values = new Dictionary<string, string>
                {
                    { "username", user },
                    { "password", pass },
                    { "client", "referer" },
                    { "referer", "http://localhost:8888"},
                    { "f", "json"},
                    { "expiration", expiry.ToString()}
                };
                
                var content = new FormUrlEncodedContent(values);
                var response = await client.PostAsync($"{url}/sharing/rest/generateToken", content);                
                var responseString = await response.Content.ReadAsStringAsync();                
                return responseString;
            }
            catch (Exception e)
            {
                Console.Out.WriteLine("getToken Error: " + e.Message);
                //log.LogInformation("Error: " + e.Message);
                return "getToken Error: " + e.Message;
            }
        }
    }
}
