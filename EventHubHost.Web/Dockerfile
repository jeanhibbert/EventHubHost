FROM mcr.microsoft.com/dotnet/aspnet:9.0-noble AS base
WORKDIR /app
EXPOSE 8080
ENV ASPNETCORE_URLS=http://+:8080

FROM mcr.microsoft.com/dotnet/sdk:9.0-noble AS build
WORKDIR /src
COPY ["EventHubHost.Web/EventHubHost.Web.csproj", "EventHubHost.Web/"]
COPY ["EventHubHost.ServiceDefaults/EventHubHost.ServiceDefaults.csproj", "EventHubHost.ServiceDefaults/"]
RUN dotnet restore "EventHubHost.Web/EventHubHost.Web.csproj"
COPY . .
RUN dotnet publish "EventHubHost.Web/EventHubHost.Web.csproj" -c Release -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=build /app/publish .
USER $APP_UID
ENTRYPOINT ["dotnet", "EventHubHost.Web.dll"]