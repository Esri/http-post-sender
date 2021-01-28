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
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using Newtonsoft.Json.Linq;

namespace EventHubsSender
{
    class Program
    {
        
        private static string connectionString = ConfigurationManager.AppSettings["connectionString"];
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
                JObject schema =  new JObject();

                // Read and display lines from the file until the end of 
                // the file is reached.
                string[] contentArray = readStream.ReadToEnd().Replace("\r", "").Split('\n');

                readStream.Close();

                int c = contentArray.Length;
                bool runTask = true;


                //if (hasHeaders)
                //{
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
                        //Console.WriteLine($"New fields: {dictObj.Keys}");
                        dictObj.Keys.CopyTo(fields,0);
                    }
                //}
                
                string connectionSubstring = connectionString.Substring(0,connectionString.LastIndexOf(';'));
                Console.WriteLine($"Event hub connection string: {connectionSubstring}");
                string eventHubName = connectionString.Substring(connectionString.LastIndexOf('=')+1);
                Console.WriteLine($"Event hub name (entity path): {eventHubName}");

                //topicClient = new TopicClient(ServiceBusConnectionString, TopicName);
                // Create a producer client that you can use to send events to an event hub
                await using (var producerClient = new EventHubProducerClient(connectionSubstring, eventHubName))
                {
                    int count = 0;
                    int countTotal = 0;
                    EventDataBatch eventBatch = null;
                    //string messageBody = "";
                    
                    var stopwatch = new Stopwatch();
                    while (runTask)
                    {
                        for (int l = (hasHeaders ? 1 : 0); l < c; l++)
                        {
                            line = contentArray[l];

                            // Create a batch of events if needed
                            if (eventBatch == null)
                            {
                                eventBatch = await producerClient.CreateBatchAsync();
                                stopwatch.Start();
                            }
                            eventBatch = eventBatch ?? await producerClient.CreateBatchAsync();
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
                            //Console.WriteLine(schema.ToString());
                            count++;

                            // Add events to the batch. An event is a represented by a collection of bytes and metadata. 

                            eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(schema.ToString())));
                            //messageBody = messageBody + schema.ToString()+"\n";
                            if (count == numLinesPerBatch)
                            {

                                // Use the producer client to send the batch of events to the event hub
                                await producerClient.SendAsync(eventBatch);
                                //var message = new Message(Encoding.UTF8.GetBytes(messageBody));
                                //await topicClient.SendAsync(message);
                                countTotal += count;
                                eventBatch = null;
                                //messageBody = "";
                                stopwatch.Stop();
                                int elapsed_time = (int)stopwatch.ElapsedMilliseconds;
                                stopwatch.Reset();
                                //Console.WriteLine(string.Format("A batch of {0} events has been published. It took {1} milliseconds. Total sent: {2}.", count, elapsed_time, countTotal));
                                if (elapsed_time < sendInterval) {
                                    Console.WriteLine(string.Format("A batch of {0} events has been published. It took {1} milliseconds. Waiting for {2} milliseconds. Total sent: {3}.", count, elapsed_time, sendInterval - elapsed_time, countTotal));
                                    Thread.Sleep(sendInterval - elapsed_time);
                                }
                                else
                                {
                                    Console.WriteLine(string.Format("A batch of {0} events has been published. It took {1} milliseconds.  Total sent: {2}.", count, elapsed_time, countTotal));
                                }
                                count = 0;

                            }
                        }
                        Console.WriteLine(string.Format("Reached the end of the simulation file. Repeat is set to {0}", (repeatSimulation)));
                        if (!repeatSimulation)
                        {
                            runTask = false;
                        }
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
    }
}
