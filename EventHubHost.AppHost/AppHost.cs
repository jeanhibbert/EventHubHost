var builder = DistributedApplication.CreateBuilder(args);

var sql = builder.AddSqlServer("sqlserver")
    .WithDataVolume("eventhubhost-sqlserver-data");

var eventDb = sql.AddDatabase("eventdb");

var vectorDb = builder.AddContainer("vectordb", "qdrant/qdrant", "v1.13.4")
    .WithHttpEndpoint(targetPort: 6333, name: "http")
    .WithVolume("eventhubhost-qdrant-data", "/qdrant/storage");

var llm = builder.AddContainer("llm", "ollama/ollama", "0.5.7")
    .WithHttpEndpoint(targetPort: 11434, name: "http")
    .WithVolume("eventhubhost-ollama-data", "/root/.ollama");

var apiService = builder.AddProject<Projects.EventHubHost_ApiService>("apiservice")
    .WithHttpHealthCheck("/health")
    .WithReference(eventDb)
    .WithEnvironment("Qdrant__Endpoint", vectorDb.GetEndpoint("http"))
    .WithEnvironment("Ollama__Endpoint", llm.GetEndpoint("http"))
    .WaitFor(eventDb)
    .WaitFor(vectorDb)
    .WaitFor(llm);

builder.AddProject<Projects.EventHubHost_Web>("webfrontend")
    .WithExternalHttpEndpoints()
    .WithHttpHealthCheck("/health")
    .WithReference(apiService)
    .WaitFor(apiService);

builder.Build().Run();
