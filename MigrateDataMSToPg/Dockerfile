FROM mcr.microsoft.com/dotnet/runtime:6.0 AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:6.0 AS build
ARG BUILD_CONFIGURATION=Release
WORKDIR /src
COPY ["MigrateDataMSToPg/MigrateDataMSToPg.csproj", "MigrateDataMSToPg/"]
RUN dotnet restore "MigrateDataMSToPg/MigrateDataMSToPg.csproj"
COPY . .
WORKDIR "/src/MigrateDataMSToPg"
RUN dotnet build "MigrateDataMSToPg.csproj" -c $BUILD_CONFIGURATION -o /app/build

FROM build AS publish
ARG BUILD_CONFIGURATION=Release
RUN dotnet publish "MigrateDataMSToPg.csproj" -c $BUILD_CONFIGURATION -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "MigrateDataMSToPg.dll"]
