using Amazon.Runtime.SharedInterfaces;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.Textract;
using Amazon.Textract.Model;
using System.Text.Json;

namespace Scratch
{
    internal class Program
    {
        private static int MAX_REQUESTS = 10;
        private static Dictionary<string, string> fileJobMap = new Dictionary<string, string>();
        static async Task Main(string[] args)
        {
            SummarizeResults();
            return;

            AmazonS3Client client = new AmazonS3Client();
            AmazonTextractClient amazonTextractClient = new AmazonTextractClient();

            var bucketName = "textract-console-us-east-2-a88e6f2e-e02e-4010-b10e-a2b83471239d";
            //var bucketName = "textract-console-us-east-2-a88e6f2e-e02e-4010-b10e-a2b83471239d.s3.us-east-2.amazonaws.com";
            var listObjectRequest = new ListObjectsV2Request();
            listObjectRequest.BucketName = bucketName;
            listObjectRequest.Prefix = "documents/";
            listObjectRequest.Delimiter = "/";


            var documentListResponse = await client.ListObjectsV2Async(listObjectRequest);
            int count = 0;
            List<string> jobIds = new List<string>();
            foreach (var document in documentListResponse.S3Objects)
            {
                if (document.Key != listObjectRequest.Prefix)
                {
                    var jobId = await SendForAnalysis(amazonTextractClient, document);
                    if (!string.IsNullOrEmpty(jobId))
                    {
                        jobIds.Add(jobId);
                        count++;
                        fileJobMap.Add(jobId, document.Key);
                    }
                    
                    if (count >= MAX_REQUESTS)
                        break;
                }
            }

            List<Task<KeyValuePair<GetDocumentAnalysisResponse, string>>> pollingTasks = new List<Task<KeyValuePair<GetDocumentAnalysisResponse, string>>>();
            foreach(var jobid in jobIds)
            {
                pollingTasks.Add(PollForResults(amazonTextractClient, jobid));
            }

            while(pollingTasks.Count > 0)
            {
                await Task.WhenAny(pollingTasks);
                var temp = pollingTasks.Where(t => !t.IsCompleted).ToList();
                var results = pollingTasks.Where(t => t.IsCompleted).Select(t => t.Result);
                ProcessResults(results);
                pollingTasks = temp;
            }


            Console.WriteLine("Done");
        }

        private static void ProcessResults(IEnumerable<KeyValuePair<GetDocumentAnalysisResponse, string>> results)
        {
            foreach(var result in results)
            {
                var queryResults = result.Key.Blocks.Where(b => b.BlockType == BlockType.QUERY_RESULT
                                                        || b.BlockType == BlockType.QUERY).ToList();
                var json = JsonSerializer.Serialize(queryResults);
                File.WriteAllText($"C:\\Temp\\Textract\\Results\\{fileJobMap[result.Value]}.json", json);
            }
        }

        private static async Task<KeyValuePair<GetDocumentAnalysisResponse, string>> PollForResults(AmazonTextractClient client, string jobId)
        {
            var request = new GetDocumentAnalysisRequest
            {
                JobId = jobId
            };

            var completed = false;

            while (!completed)
            {
                var response = await client.GetDocumentAnalysisAsync(request);
                completed = response.JobStatus != JobStatus.IN_PROGRESS;
                if (completed)
                    return new KeyValuePair<GetDocumentAnalysisResponse, string>(response, jobId);

                await Task.Delay(5000);
            }

            return new KeyValuePair<GetDocumentAnalysisResponse, string>();
        }

        private static async Task<string> SendForAnalysis(AmazonTextractClient client, Amazon.S3.Model.S3Object document)
        {
            if (document.Key != "documents/" && document.Key.StartsWith("documents/"))
            {
                var request = BuildAnalyzeRequest(document.BucketName, document.Key);
                var response = await client.StartDocumentAnalysisAsync(request);

                return response.JobId;
            }
                
            return "";
        }

        private static List<Query> Queries = new List<Query>
        {
            new Query
            {
                Text = "What is the beneficiary bank name?",
                Alias = "Beneficiary Bank",
                Pages = new List<string> {"*"}
            },
            new Query
            {
                Text = "What is the beneficiary bank ABA number?",
                Alias = "ABA",
                Pages = new List<string> {"*"}
            },
            new Query
            {
                Text = "What is the loan number?",
                Alias = "Loan Number",
                Pages = new List<string> {"*"}
            },
            new Query
            {
                Text = "What account should funds be wired to?",
                Alias = "Account Number",
                Pages = new List<string> {"*"}
            },
        };

        public static StartDocumentAnalysisRequest BuildAnalyzeRequest(string bucket, string fileName)
        {
            var analyzeDocumentRequest = new StartDocumentAnalysisRequest()
            {
                DocumentLocation = new DocumentLocation
                {
                    S3Object = new Amazon.Textract.Model.S3Object
                    {
                        Bucket = bucket,
                        Name = fileName
                    }
                },
                FeatureTypes = new List<String> { "QUERIES" },
                QueriesConfig = new QueriesConfig {  Queries = Queries }
            };

            return analyzeDocumentRequest;
        }

        private static void SummarizeResults()
        {
            var files = Directory.GetFiles(@"C:\Temp\Textract\Results\documents");
            List<QueryResult> queryResults = new List<QueryResult>();
            foreach(var file in files)
            {
                var fileJson = File.ReadAllText(file);
                var blocks = JsonSerializer.Deserialize<List<Block>>(fileJson);
                var queryBlocks = blocks.Where(b => b.BlockType == BlockType.QUERY);
                foreach(var qb in  queryBlocks)
                {
                    var answer = blocks.FirstOrDefault(b => b.Id == qb.Relationships.FirstOrDefault(r => r.Type == RelationshipType.ANSWER)?.Ids.FirstOrDefault());
                    var result = new QueryResult
                    {
                        FileName = file,
                        Page = qb.Page,
                        Question = qb.Query.Text,
                        Answer = answer?.Text ?? "",
                        Confidence = answer?.Confidence ?? 0
                    };
                    queryResults.Add(result);
                }
            }

            var orderedResults = queryResults.OrderBy(qr => qr.FileName).ThenBy(qr => qr.Question).ThenBy(qr => qr.Page);
            var summaryJson = JsonSerializer.Serialize(orderedResults);
            File.WriteAllText(@"C:\Temp\Textract\summary.json", summaryJson);
        }

        public class QueryResult
        {
            public string FileName { get; set; }
            public string Question { get; set; }
            public string Answer { get; set; }
            public int Page { get; set; }
            public float Confidence { get; set; }
        }
    }
}
