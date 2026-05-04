var builder = DistributedApplication.CreateBuilder(args);

var sql = builder.AddSqlServer("sqlserver")
    .WithDataVolume("eventhubhost-sqlserver-data");

var eventDb = sql.AddDatabase("eventdb");

var llm = builder.AddContainer("llm", "ollama/ollama", "0.5.7")
    .WithHttpEndpoint(targetPort: 11434, name: "http")
    .WithVolume("eventhubhost-ollama-data", "/root/.ollama");

var apiService = builder.AddProject<Projects.EventHubHost_ApiService>("apiservice")
    .WithHttpHealthCheck("/health")
    .WithReference(eventDb)
    .WithEnvironment("Ollama__Endpoint", llm.GetEndpoint("http"))
    .WaitFor(eventDb)
    .WaitFor(llm);

builder.AddProject<Projects.EventHubHost_Web>("webfrontend")
    .WithExternalHttpEndpoints()
    .WithHttpHealthCheck("/health")
    .WithReference(apiService)
    .WaitFor(apiService);

builder.Build().Run();
