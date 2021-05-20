using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Company.Function
{
    public static class BlobTriggerCharp
    {
        [FunctionName("BlobTriggerCharp")]
        public static async Task Run(
            [BlobTrigger("csv/{id}-{name}.csv", Connection = "challenge06_STORAGE")] Stream myBlob,
            [DurableClient] IDurableClient entityClient,
            string name,
            string id,
            ILogger log)
        {
            // myBlob.
            log.LogInformation($"C# Blob trigger function Processed blob\n Name:{name} \n Size: {myBlob.Length} Bytes");

            var entityId = new EntityId("Counter", id);
            log.LogInformation(name);
            await entityClient.SignalEntityAsync(entityId, "add", 0);
        }

        public class DTO
        {
            public string orderHeaderDetailsCSVUrl { get; set; }
            public string orderLineItemsCSVUrl { get; set; }
            public string productInformationCSVUrl { get; set; }
        }

        [FunctionName("Counter")]
        public static async Task Counter(
            [EntityTrigger] IDurableEntityContext ctx,
            [CosmosDB(
                databaseName: "Ratings",
                collectionName: "Rating",
                ConnectionStringSetting = "connectionStringSetting")]
                IAsyncCollector<dynamic> ratingsCollection,
            ILogger log)
        {
            int cpt;
            log.LogWarning("Start");

            cpt = ctx.GetState<int>() + 1;
            ctx.SetState(cpt);

            log.LogInformation(cpt.ToString());
            log.LogWarning($"WIP {cpt} !");

            if (cpt == 3)
            {
                log.LogError("youpi !");

                using (HttpClient httpClient = new HttpClient())
                {
                    var entityId = ctx.EntityId.EntityKey;
                    var json = JsonConvert.SerializeObject(new DTO
                    {
                        orderHeaderDetailsCSVUrl = $"https://challenge06.blob.core.windows.net/csv/{entityId}-OrderHeaderDetails.csv",
                        orderLineItemsCSVUrl = $"https://challenge06.blob.core.windows.net/csv/{entityId}-OrderLineItems.csv",
                        productInformationCSVUrl = $"https://challenge06.blob.core.windows.net/csv/{entityId}-ProductInformation.csv",
                    });
                    var stringContent = new StringContent(json, UnicodeEncoding.UTF8, "application/json");

                    log.LogTrace(json.ToString());

                    var res = await httpClient.PostAsync("https://serverlessohmanagementapi.trafficmanager.net/api/order/combineOrderContent", stringContent);

                    if (res.IsSuccessStatusCode)
                    {
                        
                        var tmp = await res.Content.ReadAsStringAsync();
                        log.LogWarning(tmp);
                        
                        // dynamic data = JsonSerializer.DeserializeObject<dynamic>(tmp);
                        List<dynamic> data = JsonConvert.DeserializeObject<List<dynamic>>(tmp);
                        // dynamic data = JsonConvert.DeserializeObject<List<dynamic>>(tmp);
                        // dynamic data = JObject.Parse(tmp);
                        data.ForEach(async d => await ratingsCollection.AddAsync(d));
                         
                        //  await ratingsCollection.AddAsync(data);// .Result;
                        log.LogError("youpi youpi !");
                        // log.LogError($"{res.StatusCode}\n\t" + res.Content.ReadAsStringAsync().Result);
                    }
                    else
                    {
                        log.LogError($"nop :( {res.StatusCode}\n\t" + res.Content.ReadAsStringAsync().Result);
                    }

                    log.LogError(res.IsSuccessStatusCode.ToString());
                }
            }
            else if (cpt > 3)
            {
                throw new Exception($"cpt = {cpt}");
            }
        }

    }
}
