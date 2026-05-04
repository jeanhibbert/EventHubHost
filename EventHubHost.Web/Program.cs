using EventHubHost.Web;
using EventHubHost.Web.Components;

var builder = WebApplication.CreateBuilder(args);

// Add service defaults & Aspire client integrations.
builder.AddServiceDefaults();

// Add services to the container.
builder.Services.AddRazorComponents()
    .AddInteractiveServerComponents();

builder.Services.AddOutputCache();

builder.Services.Configure<CorrelationApiOptions>(options =>
{
    var apiEndpoint = builder.Configuration["services:apiservice:http:0"]
        ?? builder.Configuration["services:apiservice:https:0"]
        ?? "http://localhost:5562";
    options.EventHubUrl = $"{apiEndpoint.TrimEnd('/')}/hubs/events";
});

builder.Services.AddHttpClient<CorrelationApiClient>(client =>
    {
        // This URL uses "https+http://" to indicate HTTPS is preferred over HTTP.
        // Learn more about service discovery scheme resolution at https://aka.ms/dotnet/sdschemes.
        client.BaseAddress = new("https+http://apiservice");
        // The API may wait on a self-hosted LLM for several minutes. The default standard
        // resilience handler caps requests at ~30 seconds, so override its timeouts here.
        client.Timeout = TimeSpan.FromMinutes(10);
    })
    .AddStandardResilienceHandler(options =>
    {
        options.AttemptTimeout.Timeout = TimeSpan.FromMinutes(10);
        options.TotalRequestTimeout.Timeout = TimeSpan.FromMinutes(10);
        options.CircuitBreaker.SamplingDuration = TimeSpan.FromMinutes(20);
    });

var app = builder.Build();

if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/Error", createScopeForErrors: true);
    // The default HSTS value is 30 days. You may want to change this for production scenarios, see https://aka.ms/aspnetcore-hsts.
    app.UseHsts();
}

app.UseHttpsRedirection();

app.UseAntiforgery();

app.UseOutputCache();

app.MapStaticAssets();

app.MapRazorComponents<App>()
    .AddInteractiveServerRenderMode();

app.MapDefaultEndpoints();

app.Run();
