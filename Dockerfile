FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/incident-copilot ./app/cmd/incident-copilot

FROM gcr.io/distroless/static-debian12
COPY --from=build /out/incident-copilot /incident-copilot
EXPOSE 8080
ENTRYPOINT ["/incident-copilot"]
