package internal

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net/url"
	"os"
	"time"

	"github.com/open-telemetry/opamp-go/protobufs"
	"github.com/shirou/gopsutil/process"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/metric"
	controller "go.opentelemetry.io/otel/sdk/metric/controller/basic"
	processor "go.opentelemetry.io/otel/sdk/metric/processor/basic"
	"go.opentelemetry.io/otel/sdk/metric/selector/simple"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
)

type MetricReporter struct {
	logger          *log.Logger
	meter           metric.Meter
	meterShutdowner func()
	done            chan struct{}

	process               *process.Process
	processMemoryPhysical metric.Int64ValueObserver
	counter               metric.Int64Counter
	processCpuTime        metric.Float64SumObserver
}

func NewMetricReporter(
	logger *log.Logger,
	dest *protobufs.ConnectionSettings,
	clientCert *tls.Certificate,
	agent *Agent,
) (*MetricReporter, error) {
	if dest.DestinationEndpoint == "" {
		err := fmt.Errorf("metric destination must specify HttpUrl")
		return nil, err
	}
	u, err := url.Parse(dest.DestinationEndpoint)
	if err != nil {
		err := fmt.Errorf("invalid HttpUrl: %v", err)
		return nil, err
	}

	opts := []otlpmetrichttp.Option{
		otlpmetrichttp.WithEndpoint(u.Host),
		otlpmetrichttp.WithURLPath(u.Path),
	}

	if u.Scheme == "http" {
		opts = append(opts, otlpmetrichttp.WithInsecure())
	}

	if clientCert != nil {
		opts = append(opts, otlpmetrichttp.WithTLSClientConfig(&tls.Config{
			Certificates: []tls.Certificate{*clientCert},
		}))
	}

	client := otlpmetrichttp.NewClient(opts...)

	metricExporter, err := otlpmetric.New(context.Background(), client)
	if err != nil {
		err := fmt.Errorf("failed to initialize stdoutmetric export pipeline: %v", err)
		return nil, err
	}

	resource, err := resource.New(context.Background(),
		resource.WithAttributes(
			semconv.ServiceNameKey.String(agent.agentType),
			semconv.ServiceVersionKey.String(agent.agentVersion),
			semconv.ServiceInstanceIDKey.String(agent.instanceId.String()),
		),
	)

	c := controller.New(
		processor.New(
			simple.NewWithExactDistribution(),
			metricExporter,
		),
		controller.WithExporter(metricExporter),
		controller.WithCollectPeriod(5*time.Second),
		controller.WithResource(resource),
	)

	err = c.Start(context.Background())
	if err != nil {
		err := fmt.Errorf("failed to initialize metric controller: %v", err)
		return nil, err
	}

	provider := c.MeterProvider()

	reporter := &MetricReporter{}

	reporter.done = make(chan struct{})

	reporter.meter = provider.Meter("opamp")

	reporter.process, err = process.NewProcess(int32(os.Getpid()))
	if err != nil {
		err := fmt.Errorf("cannot query own process: %v", err)
		return nil, err
	}

	reporter.processCpuTime = metric.Must(reporter.meter).NewFloat64SumObserver(
		"process.cpu.time",
		reporter.processCpuTimeFunc,
	)

	reporter.processMemoryPhysical = metric.Must(reporter.meter).NewInt64ValueObserver(
		"process.memory.physical_usage",
		reporter.processMemoryPhysicalFunc,
	)

	reporter.counter = metric.Must(reporter.meter).NewInt64Counter("custom_metric_ticks")

	reporter.meterShutdowner = func() { _ = c.Stop(context.Background()) }

	go reporter.sendMetrics()

	return reporter, nil
}

func (reporter *MetricReporter) processCpuTimeFunc(ctx context.Context, result metric.Float64ObserverResult) {
	times, err := reporter.process.Times()
	if err != nil {
		reporter.logger.Printf("Cannot get process CPU times: %v", err)
	}

	result.Observe(math.Min(times.User+rand.Float64(), 1), attribute.String("state", "user"))
	result.Observe(math.Min(times.System+rand.Float64(), 1), attribute.String("state", "system"))
	result.Observe(math.Min(times.Iowait+rand.Float64(), 1), attribute.String("state", "wait"))
}

func (reporter *MetricReporter) processMemoryPhysicalFunc(ctx context.Context, result metric.Int64ObserverResult) {
	memory, err := reporter.process.MemoryInfo()
	if err != nil {
		reporter.logger.Printf("Cannot get process memory information: %v", err)
		return
	}
	result.Observe(int64(memory.RSS) + rand.Int63n(10000000))
}

func (reporter *MetricReporter) sendMetrics() {
	t := time.NewTicker(time.Second * 5)
	ticks := int64(0)

	for {
		select {
		case <-reporter.done:
			return

		case <-t.C:
			ctx := context.Background()
			reporter.meter.RecordBatch(
				ctx,
				[]attribute.KeyValue{},
				reporter.counter.Measurement(ticks),
			)
			ticks++
		}
	}
}

func (reporter *MetricReporter) Shutdown() {
	if reporter.done != nil {
		close(reporter.done)
	}

	if reporter.meterShutdowner != nil {
		reporter.meterShutdowner()
	}
}
