# Taken from/inspired by:
# https://hamy.xyz/labs/2022-10-run-fsharp-dotnet-in-docker-console-app

# **Build Project**
# https://hub.docker.com/_/microsoft-dotnet
FROM mcr.microsoft.com/dotnet/sdk:5.0 AS build
EXPOSE 80

WORKDIR /source

# Copy fsproj and restore all dependencies
COPY ./Crdt/*.fsproj ./
RUN dotnet restore

# Copy source code and build / publish app and libraries
COPY . .
RUN dotnet publish -c release -o /app

# **Run project**
# Create new layer with runtime, copy app / libraries, then run dotnet
FROM mcr.microsoft.com/dotnet/aspnet:5.0
WORKDIR /app
COPY --from=build /app .
#ENTRYPOINT ["dotnet", "Crdt.Tests.dll"]
CMD ["/bin/bash"]
